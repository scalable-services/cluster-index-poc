package cluster

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.Producer
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import cluster.ClusterCommands.RangeCommand
import cluster.grpc.{ClusterClientResponseServiceHandler, HelloReply, KeyIndexContext, RangeTaskResponse}
import cluster.helpers.{TestConfig, TestHelper}
import com.datastax.oss.driver.api.core.CqlSession
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}
import services.scalable.index.grpc.IndexContext
import services.scalable.index.{Bytes, Commands, IndexBuilder, QueryableIndex}

import java.util.UUID
import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.util.hashing.MurmurHash3

class ClusterClient[K, V](val metaCtx: IndexContext)(implicit val metaBuilder: IndexBuilder[K, KeyIndexContext],
                                                     val session: CqlSession,
                                                     val rangeCommandSerializer: GrpcRangeCommandSerializer[K, V]){
  import metaBuilder._

  var client_uuid = UUID.randomUUID.toString
  var port = "0"

  val conf =
    ConfigFactory.parseString("akka.http.server.enable-http2 = on").withFallback(ConfigFactory.defaultApplication())
  implicit  val system = ActorSystem.create(s"client-${UUID.randomUUID.toString}", conf)

  val producerSettings = ProducerSettings[String, Bytes](system, new StringSerializer, new ByteArraySerializer)
    .withBootstrapServers("localhost:9092")

  val kafkaProducer = producerSettings.createKafkaProducer()
  val settingsWithProducer = producerSettings.withProducer(kafkaProducer)

  val rangeTasks = TrieMap.empty[String, (RangeCommand[K, V], Promise[RangeTaskResponse])]

   def normalize(commands: Seq[Commands.Command[K, V]], version: Option[String]): Seq[Commands.Command[K, V]] = {
    commands.groupBy(_.indexId).map { case (indexId, cmds) =>

      val insertions = cmds.filter(_.isInstanceOf[Commands.Insert[K, V]]).map(_.asInstanceOf[Commands.Insert[K, V]])
        .map { c => c.list}.flatten.map{ case (k, v, vs) => k -> (v, vs)}.toMap
      val updates = cmds.filter(_.isInstanceOf[Commands.Update[K, V]]).map(_.asInstanceOf[Commands.Update[K, V]])
        .map { c => c.list}.flatten.map{ case (k, v, vs) => k -> (v, vs) }.toMap
      val removals = cmds.filter(_.isInstanceOf[Commands.Remove[K, V]]).map(_.asInstanceOf[Commands.Remove[K, V]])
        .map { c => c.keys}.flatten.map{ case (k, vs) => k -> (k, vs) }.toMap

      // Remove all insertion keys that are in removal set (does not make sense insert what you will remove...)
      var insertionsn = insertions.filterNot { e =>
        removals.isDefinedAt(e._1)
      }

      // Filter out updates that are in removal set...
      var updatesn = updates.filterNot { e =>
        removals.isDefinedAt(e._1)
      }

      // Update all insertion operations that are in update set...
      insertionsn = insertionsn.filter { e => updatesn.isDefinedAt(e._1)}.map { e =>
        e._1 -> (updatesn(e._1)._1, e._2._2)
      } ++ insertionsn.filterNot{ e => updatesn.isDefinedAt(e._1) }

      // Remove update operations that were in insertions (updates on inserting keys are actually insertions...)
      updatesn = updatesn.filterNot { e =>
        insertionsn.isDefinedAt(e._1)
      }

      // Remove keys which were in insert operations from removal set
      val removalsn = removals.filterNot { e =>
        insertions.isDefinedAt(e._1)
      }

      Seq(
        Commands.Remove[K, V](indexId, removalsn.values.toSeq, version),
        Commands.Insert[K, V](indexId, insertionsn.map{ case (k, list) => Tuple3(k, list._1, list._2) }.toSeq, version),
        Commands.Update[K, V](indexId, updatesn.map{ case (k, list) => Tuple3(k, list._1, list._2) }.toSeq, version)
      )
    }.flatten.toSeq
  }

  def sendTasks(tasks: Seq[RangeCommand[K, V]]): Future[Boolean] /*Future[Seq[(RangeCommand[K, V], Boolean)]]*/ = {
    if(tasks.isEmpty) return Future.successful(true)

    val records = tasks.map { rc =>

      val id = MurmurHash3.stringHash(rc.rangeId).abs % TestConfig.N_PARTITIONS
      new ProducerRecord[String, Bytes](s"${TestConfig.RANGE_INDEX_TOPIC}-${id}", rc.id,
        rangeCommandSerializer.serialize(rc))
    }

    val futures = tasks.map { t =>
      val promise = Promise[RangeTaskResponse]()
      rangeTasks.put(t.id, t -> promise)
      promise.future.map{t -> _}
    }

    Source(records)
      .runWith(Producer.plainSink(settingsWithProducer))
      .flatMap(_ => Future.sequence(futures))
      .flatMap {
        case responses if responses.exists(_._2.hasRootChanged) => TestHelper.loadIndex(metaCtx.id).map(_.get).map { ctx =>
          meta = new QueryableIndex[K, KeyIndexContext](ctx)(metaBuilder)
          responses
        }
        case responses => Future.successful(responses)
      }
      .flatMap {
        case results if results.forall(_._2.ok == true) => Future.successful(true)
        case results =>
          val succeed = results.filter(_._2.ok).map(_._1)
          val notSucceed = results.filterNot(_._2.ok).map(_._1)

          println(s"${Console.YELLOW_B} TRYING COMMANDS AGAIN... SUCCEED: ${succeed.length} not succeed: ${notSucceed.length} ${Console.RESET}")

          val meta1 = Await.result(TestHelper.loadIndex(TestConfig.CLUSTER_INDEX_NAME), Duration.Inf).get
          val metai = new QueryableIndex[K, KeyIndexContext](meta1)(metaBuilder)
          val inOrder = Await.result(TestHelper.all(metai.inOrder()), Duration.Inf)

          val rn = inOrder.filter { x =>
            notSucceed.exists{y => y.rangeId == x._2.rangeId}
          }

          val beforeAndNow = notSucceed.map { c =>
            (c.rangeId, rn.find(_._2.rangeId == c.rangeId).get._2.lastChangeVersion, c.lastChangeVersion)
          }

          println(s"${Console.CYAN_B} before and now: ${beforeAndNow} ${Console.RESET}")

          TestHelper.loadIndex(metaCtx.id).map(_.get).flatMap { freshCtx =>
            //val client = new ClusterClient[K, V](freshCtx)

            meta = new QueryableIndex[K, KeyIndexContext](freshCtx)(metaBuilder)
            val client = this

            client.execute(notSucceed.map(_.commands).flatten).flatMap { rangeCmds =>

              val rc = rangeCmds.values.toSeq

              println(s"${Console.MAGENTA_B}[${client.client_uuid}] task ids: ${rc.map(c => (c.id, c.rangeId, c.lastChangeVersion))}${Console.RESET}...")

              client.sendTasks(rc)
            }
          }
      }
  }

  var meta = new QueryableIndex[K, KeyIndexContext](metaCtx)(metaBuilder)

  def findRange(k: K): Future[Option[(K, KeyIndexContext, String)]] = {
    meta.findPath(k).map {
      case None => None
      case Some(leaf) => Some(leaf.findPath(k)._2).map { x =>
        (x._1, x._2, x._3)
      }
    }
  }

  def sliceInsertion(c: Commands.Insert[K, V])(ranges: TrieMap[String, (Tuple3[K, String, String], Seq[Commands.Command[K, V]])]): Future[Int] = {
    val sorted = c.list.sortBy(_._1)

    val len = sorted.length
    //val insertions = TrieMap.empty[String, Seq[Commands.Insert[K, V]]]

    def insert(pos: Int): Future[Int] = {
      if (pos == len) return Future.successful(sorted.length)

      var list = sorted.slice(pos, len)
      val (k, _, vs) = list(0)

      //println(s"key to find: ${new String(k.asInstanceOf[Bytes])}")

      findRange(k).map {
        case None =>
          //println("none")
          list.length
        case Some((last, rctx, vs)) =>

          val idx = list.indexWhere { case (k, _, _) => ord.gt(k, last) }
          if (idx > 0) list = list.slice(0, idx)

          //println(s"${Thread.currentThread().threadId()} seeking range for slice from ${pos}: ${list.map{x => new String(x._1.asInstanceOf[Bytes])}}")

          val cmd = Commands.Insert[K, V](c.indexId, list, c.version)

          ranges.get(rctx.rangeId) match {
            case None => ranges.put(rctx.rangeId, Tuple3(last, vs, rctx.lastChangeVersion) -> Seq(cmd))
            case Some((lastCV, cmdList)) => ranges.update(rctx.rangeId, lastCV -> (cmdList :+ cmd))
          }

          list.length
      }.flatMap { n =>
        assert(n > 0)
        //println(s"passed ${Thread.currentThread().threadId()} ${n}...")
       // pos += n
        insert(pos + n)
      }
    }

    insert(0)
  }

  def sliceRemoval(c: Commands.Remove[K, V])(ranges: TrieMap[String, (Tuple3[K, String, String], Seq[Commands.Command[K, V]])]): Future[Int] = {
    val sorted = c.keys.sorted

    val len = sorted.length
    var pos = 0

    //val insertions = TrieMap.empty[String, Seq[Commands.Insert[K, V]]]

    def remove(): Future[Int] = {
      if (pos == len) return Future.successful(sorted.length)

      var list = sorted.slice(pos, len)
      val (k, _) = list(0)

      findRange(k).map {
        case None => list.length
        case Some((last, rctx, vs)) =>

          val idx = list.indexWhere { case (k, vs) => ord.gt(k, last) }
          if (idx > 0) list = list.slice(0, idx)

          val cmd = Commands.Remove[K, V](c.indexId, list, c.version)

          ranges.get(rctx.rangeId) match {
            case None => ranges.put(rctx.rangeId, Tuple3(last, vs, rctx.lastChangeVersion) -> Seq(cmd))
            case Some((lastCV, cmdList)) => ranges.update(rctx.rangeId, lastCV -> (cmdList :+ cmd))
          }

          list.length
      }.flatMap { n =>
        pos += n
        remove()
      }
    }

    remove()
  }

  def sliceUpdate(c: Commands.Update[K, V])(ranges: TrieMap[String, (Tuple3[K, String, String], Seq[Commands.Command[K, V]])]): Future[Int] = {
    val sorted = c.list.sortBy(_._1)

    val len = sorted.length
    var pos = 0

    //val insertions = TrieMap.empty[String, Seq[Commands.Insert[K, V]]]

    def update(): Future[Int] = {
      if (pos == len) return Future.successful(sorted.length)

      var list = sorted.slice(pos, len)
      val (k, _, _) = list(0)

      findRange(k).map {
        case None => list.length
        case Some((last, rctx, vs)) =>

          val idx = list.indexWhere { case (k, _, _) => ord.gt(k, last) }
          if (idx > 0) list = list.slice(0, idx)

          val cmd = Commands.Update[K, V](c.indexId, list, c.version)

          ranges.get(rctx.rangeId) match {
            case None => ranges.put(rctx.rangeId, Tuple3(last, vs, rctx.lastChangeVersion) -> Seq(cmd))
            case Some((lastCV, cmdList)) => ranges.update(rctx.rangeId, lastCV -> (cmdList :+ cmd))
          }

          list.length
      }.flatMap { n =>
        pos += n
        update()
      }
    }

    update()
  }

  def execute(commands: Seq[Commands.Command[K, V]]): Future[TrieMap[String, RangeCommand[K, V]]] = {
    val ranges = TrieMap.empty[String, (Tuple3[K, String, String], Seq[Commands.Command[K, V]])]

    commands.foreach {
      case c: Commands.Insert[K, V] => Await.result(sliceInsertion(c)(ranges), Duration.Inf)
      case c: Commands.Update[K, V] => Await.result(sliceUpdate(c)(ranges), Duration.Inf)
      case c: Commands.Remove[K, V] => Await.result(sliceRemoval(c)(ranges), Duration.Inf)
    }

    Future.successful(ranges.map{case (rangeId, (keyCtx, list)) => rangeId ->
      RangeCommand(UUID.randomUUID.toString, rangeId, metaCtx.id, list, keyCtx._1 -> keyCtx._2, keyCtx._3, port)})
  }

  //val system = ActorSystem(s"client-${client_uuid}", conf)

  // Create service handlers
  val service: HttpRequest => Future[HttpResponse] =
    ClusterClientResponseServiceHandler(new ClusterCliSvcImpl() {
      override def respond(r: RangeTaskResponse): Future[HelloReply] = {

        //val r = Any.parseFrom(in.record.value()).unpack(RangeTaskResponse)

        println(s"client ${client_uuid} receiving ${r.id} ok: ${r.ok}")

        rangeTasks.remove(r.id).map(_._2.success(r))

        Future.successful(HelloReply(r.id, r.ok))
      }
    })

  def start(): Future[Boolean] = {
    // Bind service handler servers to localhost:8080/8081
    val binding = Http(system).newServerAt("127.0.0.1", 0).bind(service)

    // Akka boot up code
    implicit val mat = Materializer(system)

    binding.map { binding =>

      port = binding.localAddress.getPort.toString

      println(s"gRPC server bound to: ${binding.localAddress} at port ${binding.localAddress.getPort}")

      true
    }
  }

  def close(): Future[Boolean] = {
    system.terminate()
      .flatMap(_ => system.whenTerminated)
      .map(_ => true)
  }

}
