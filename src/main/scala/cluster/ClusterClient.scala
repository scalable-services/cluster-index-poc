package cluster

import akka.actor.ActorSystem
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.Producer
import akka.stream.scaladsl.Source
import cluster.ClusterCommands.RangeCommand
import cluster.grpc.{ClusterIndexCommand, KeyIndexContext, MetaTask, RangeIndexMeta, RangeTask}
import cluster.helpers.{TestConfig, TestHelper}
import com.datastax.oss.driver.api.core.CqlSession
import com.google.protobuf.any.Any
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}
import services.scalable.index.grpc.IndexContext
import services.scalable.index.{Bytes, Commands, IndexBuilder, QueryableIndex}

import java.util.UUID
import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
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

  def sendTasks(tasks: Seq[RangeCommand[K, V]]): Future[Boolean] = {
    val records = tasks.map { rc =>
      val id = MurmurHash3.stringHash(rc.rangeId).abs % TestConfig.N_PARTITIONS
      new ProducerRecord[String, Bytes](s"${TestConfig.RANGE_INDEX_TOPIC}-${id}", rc.id,
        rangeCommandSerializer.serialize(rc))
    }

    Source(records)
      .runWith(Producer.plainSink(settingsWithProducer)).map(_ => true)
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
        case Some((last, dbCtx, version)) =>

          val idx = list.indexWhere { case (k, _, _) => ord.gt(k, last) }
          if (idx > 0) list = list.slice(0, idx)

          //println(s"${Thread.currentThread().threadId()} seeking range for slice from ${pos}: ${list.map{x => new String(x._1.asInstanceOf[Bytes])}}")

          val c = Commands.Insert[K, V](dbCtx.id, list)

          ranges.get(dbCtx.id) match {
            case None => ranges.put(dbCtx.id, dbCtx.lastChangeVersion -> Seq(c))
            case Some((lastCV, cmdList)) => ranges.update(dbCtx.id, lastCV -> (cmdList :+ c))
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

          val c = Commands.Remove[K, V]("main", list)

          ranges.get(leafId.id) match {
            case None => ranges.put(leafId.id, leafId.lastChangeVersion -> Seq(c))
            case Some((lastCV, cmdList)) => ranges.update(leafId.id, lastCV -> (cmdList :+ c))
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

          val c = Commands.Update[K, V]("main", list)

          ranges.get(leafId.id) match {
            case None => ranges.put(leafId.id, leafId.lastChangeVersion -> Seq(c))
            case Some((lastCV, cmdList)) => ranges.update(leafId.id, lastCV -> (cmdList :+ c))
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

}
