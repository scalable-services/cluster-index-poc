package cluster

import akka.actor.ActorSystem
import akka.kafka.{CommitDelivery, CommitterSettings, ConsumerMessage, ConsumerSettings, ProducerSettings, Subscriptions}
import akka.kafka.scaladsl.{Committer, Consumer, Producer}
import akka.stream.scaladsl.{Sink, Source}
import cluster.ClusterCommands.RangeCommand
import cluster.grpc.{ClusterIndexCommand, KeyIndexContext, MetaTask, RangeIndexMeta, RangeTask, RangeTaskResponse}
import cluster.helpers.{TestConfig, TestHelper}
import com.datastax.oss.driver.api.core.CqlSession
import com.google.protobuf.any.Any
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer, StringDeserializer, StringSerializer}
import services.scalable.index.grpc.IndexContext
import services.scalable.index.{Bytes, Commands, IndexBuilder, QueryableIndex}

import java.util.UUID
import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, Promise}
import scala.util.hashing.MurmurHash3

class ClusterClient[K, V](val metaCtx: IndexContext)(implicit val metaBuilder: IndexBuilder[K, KeyIndexContext],
                                                     val session: CqlSession,
                                                     val rangeCommandSerializer: GrpcRangeCommandSerializer[K, V]){
  import metaBuilder._

  val system = ActorSystem.create()
  implicit val provider = system.classicSystem

  val producerSettings = ProducerSettings[String, Bytes](system, new StringSerializer, new ByteArraySerializer)
    .withBootstrapServers("localhost:9092")

  val kafkaProducer = producerSettings.createKafkaProducer()
  val settingsWithProducer = producerSettings.withProducer(kafkaProducer)

  val rangeTasks = TrieMap.empty[String, Promise[Boolean]]

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

  def sendTasks(tasks: Seq[RangeCommand[K, V]]): Future[Boolean] = {

    val records = tasks.map { rc =>
      val id = MurmurHash3.stringHash(rc.rangeId).abs % TestConfig.N_PARTITIONS
      new ProducerRecord[String, Bytes](s"${TestConfig.RANGE_INDEX_TOPIC}-${id}", rc.id,
        rangeCommandSerializer.serialize(rc))
    }

    val futures = tasks.map { t =>
      val promise = Promise[Boolean]()
      rangeTasks.put(t.id, promise)
      promise.future
    }

    Source(records)
      .runWith(Producer.plainSink(settingsWithProducer))
      .flatMap(_ => Future.sequence(futures).map(_.forall(_ == true)))
  }

  val meta = new QueryableIndex[K, KeyIndexContext](metaCtx)(metaBuilder)

  def findRange(k: K): Future[Option[(K, RangeIndexMeta, String)]] = {
    meta.findPath(k).map {
      case None => None
      case Some(leaf) => Some(leaf.findPath(k)._2).map { x =>
        (x._1, Await.result(TestHelper.getRange(x._2.rangeId), Duration.Inf).get, x._3)
      }
    }
  }

  def sliceInsertion(c: Commands.Insert[K, V])(ranges: TrieMap[String, (String, Seq[Commands.Command[K, V]])]): Future[Int] = {
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
        case Some((last, dbCtx, _)) =>

          val idx = list.indexWhere { case (k, _, _) => ord.gt(k, last) }
          if (idx > 0) list = list.slice(0, idx)

          //println(s"${Thread.currentThread().threadId()} seeking range for slice from ${pos}: ${list.map{x => new String(x._1.asInstanceOf[Bytes])}}")

          val cmd = Commands.Insert[K, V](c.indexId, list, c.version)

          ranges.get(dbCtx.id) match {
            case None => ranges.put(dbCtx.id, dbCtx.lastChangeVersion -> Seq(cmd))
            case Some((lastCV, cmdList)) => ranges.update(dbCtx.id, lastCV -> (cmdList :+ cmd))
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

  def sliceRemoval(c: Commands.Remove[K, V])(ranges: TrieMap[String, (String, Seq[Commands.Command[K, V]])]): Future[Int] = {
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
        case Some((last, leafId, version)) =>

          val idx = list.indexWhere { case (k, vs) => ord.gt(k, last) }
          if (idx > 0) list = list.slice(0, idx)

          val cmd = Commands.Remove[K, V](c.indexId, list, c.version)

          ranges.get(leafId.id) match {
            case None => ranges.put(leafId.id, leafId.lastChangeVersion -> Seq(cmd))
            case Some((lastCV, cmdList)) => ranges.update(leafId.id, lastCV -> (cmdList :+ cmd))
          }

          list.length
      }.flatMap { n =>
        pos += n
        remove()
      }
    }

    remove()
  }

  def sliceUpdate(c: Commands.Update[K, V])(ranges: TrieMap[String, (String, Seq[Commands.Command[K, V]])]): Future[Int] = {
    val sorted = c.list.sortBy(_._1)

    val len = sorted.length
    var pos = 0

    //val insertions = TrieMap.empty[String, Seq[Commands.Insert[K, V]]]

    def update(): Future[Int] = {
      if (pos == len) return Future.successful(sorted.length)

      var list = sorted.slice(pos, len)
      val (k, _, _) = list(0)

      findRange(k).map {
        case None =>
          println("none")
          list.length
        case Some((last, leafId, version)) =>

          val idx = list.indexWhere { case (k, _, _) => ord.gt(k, last) }
          if (idx > 0) list = list.slice(0, idx)

          val cmd = Commands.Update[K, V](c.indexId, list, c.version)

          ranges.get(leafId.id) match {
            case None => ranges.put(leafId.id, leafId.lastChangeVersion -> Seq(cmd))
            case Some((lastCV, cmdList)) => ranges.update(leafId.id, lastCV -> (cmdList :+ cmd))
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
    val ranges = TrieMap.empty[String, (String, Seq[Commands.Command[K, V]])]

    commands.foreach {
      case c: Commands.Insert[K, V] => Await.result(sliceInsertion(c)(ranges), Duration.Inf)
      case c: Commands.Update[K, V] => Await.result(sliceUpdate(c)(ranges), Duration.Inf)
      case c: Commands.Remove[K, V] => Await.result(sliceRemoval(c)(ranges), Duration.Inf)
    }

    Future.successful(ranges.map{case (rangeId, (lastVersion, list)) => rangeId ->
      RangeCommand(UUID.randomUUID.toString, rangeId, list, lastVersion)})
  }

  val clientId = UUID.randomUUID.toString

  val consumerSettings = ConsumerSettings[String, Array[Byte]](system, new StringDeserializer, new ByteArrayDeserializer)
    .withBootstrapServers("localhost:9092")
    .withGroupId(clientId)
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    .withClientId(s"range-task-worker-${clientId}")
  //.withPollInterval(java.time.Duration.ofMillis(10L))
  // .withStopTimeout(java.time.Duration.ofHours(1))
  //.withProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1")
  //.withStopTimeout(java.time.Duration.ofSeconds(1000L))

  val committerSettings = CommitterSettings(system).withDelivery(CommitDelivery.waitForAck)

  def handler(msg: ConsumerMessage.CommittableMessage[String, Array[Byte]]): Future[Boolean] = {
    val r = Any.parseFrom(msg.record.value()).unpack(RangeTaskResponse)

    rangeTasks.remove(r.id).map(_.success(r.ok))

    Future.successful(true)
  }

  Consumer
    .committableSource(consumerSettings, Subscriptions.topics(TestConfig.CLIENT_TOPIC))
    .mapAsync(1) { msg =>
      handler(msg).map(_ => msg.committableOffset)
    }
    .via(Committer.flow(committerSettings.withMaxBatch(1)))
    .runWith(Sink.ignore)
    .recover {
      case e: RuntimeException => e.printStackTrace()
    }

}
