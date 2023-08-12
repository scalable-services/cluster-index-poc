package cluster

import akka.actor.ActorSystem
import cluster.grpc.KeyIndexContext
import cluster.helpers.{TestConfig, TestHelper}
import services.scalable.index.impl.{CassandraStorage, DefaultCache}
import services.scalable.index.{DefaultComparators, DefaultSerializers, IndexBuilder}

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

object RangeWorkersStarter {

  var systems = Seq.empty[ActorSystem]

  def main(args: Array[String]): Unit = {

    type K = String
    type V = String

    import DefaultComparators._

    systems = (0 until TestConfig.N_PARTITIONS).map { i =>

      val session = TestHelper.getSession()
      val cache = new DefaultCache(MAX_PARENT_ENTRIES = 80000)
      //implicit val storage = new MemoryStorage(NUM_LEAF_ENTRIES, NUM_META_ENTRIES)
      implicit val storage = new CassandraStorage(session, false)

      val rangeBuilder = RangeBuilder[K, V](TestConfig.MAX_RANGE_ITEMS)(
        ordString,
        session,
        global,
        DefaultSerializers.stringSerializer,
        DefaultSerializers.stringSerializer,
        k => k,
        v => v,
        Serializers.grpcRangeCommandSerializer,
        Serializers.grpcMetaCommandSerializer
      )

      val clusterMetaBuilder = IndexBuilder.create[String, KeyIndexContext](DefaultComparators.ordString,
        DefaultSerializers.stringSerializer, Serializers.keyIndexSerializer)
        .storage(storage)
        .cache(cache)
        .serializer(Serializers.grpcStringKeyIndexContextSerializer)
        .valueToStringConverter(Printers.keyIndexContextToStringPrinter)

      new RangeWorker[K, V](s"${TestConfig.RANGE_INDEX_TOPIC}-$i", i)(rangeBuilder, clusterMetaBuilder).system
    }

    //Await.result(Future.sequence(systems.map(_.whenTerminated)), Duration.Inf)
  }

}
