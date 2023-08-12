package cluster

import akka.actor.ActorSystem
import cluster.grpc.KeyIndexContext
import cluster.helpers.{TestConfig, TestHelper}
import services.scalable.index.impl.{CassandraStorage, DefaultCache}
import services.scalable.index.{DefaultComparators, DefaultSerializers, IndexBuilder}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

object MetaWorkerStarter {

  var systems = Seq.empty[ActorSystem]

  def main(args: Array[String]): Unit = {

    val cache = new DefaultCache(MAX_PARENT_ENTRIES = 80000)
    //implicit val storage = new MemoryStorage(NUM_LEAF_ENTRIES, NUM_META_ENTRIES)
    implicit val storage = new CassandraStorage(TestHelper.getSession(), false)

    val clusterMetaBuilder = IndexBuilder.create[String, KeyIndexContext](DefaultComparators.ordString,
      DefaultSerializers.stringSerializer, Serializers.keyIndexSerializer)
      .storage(storage)
      .cache(cache)
      .serializer(Serializers.grpcStringKeyIndexContextSerializer)
      .valueToStringConverter(Printers.keyIndexContextToStringPrinter)

    systems = Seq(new MetaWorker[String, KeyIndexContext]("meta-worker")(clusterMetaBuilder,
      Serializers.grpcMetaCommandSerializer).system)

    //Await.result(Future.sequence(systems.map(_.whenTerminated)), Duration.Inf)
  }

}
