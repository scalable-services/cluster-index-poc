package cluster

import cluster.grpc.KeyIndexContext
import cluster.helpers.{TestConfig, TestHelper}
import io.netty.util.internal.ThreadLocalRandom
import org.slf4j.LoggerFactory
import services.scalable.index.impl._
import services.scalable.index.{DefaultComparators, DefaultIdGenerators, DefaultSerializers, IndexBuilder}

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object LoadIndexDemo {

  val logger = LoggerFactory.getLogger(this.getClass)

  val indexId = TestConfig.CLUSTER_INDEX_NAME //UUID.randomUUID().toString

  val rand = ThreadLocalRandom.current()

  import scala.concurrent.ExecutionContext.Implicits.global

  type K = String
  type V = String

  val NUM_LEAF_ENTRIES = TestConfig.NUM_LEAF_ENTRIES
  val NUM_META_ENTRIES = TestConfig.NUM_META_ENTRIES

  implicit val idGenerator = DefaultIdGenerators.idGenerator

  val session = TestHelper.getSession()

  implicit val cache = new DefaultCache(MAX_PARENT_ENTRIES = 80000)
  //implicit val storage = new MemoryStorage()
  implicit val storage = new CassandraStorage(session, false)

  def loadAll(): Seq[(K, V)] = {
    val metaContext = Await.result(TestHelper.loadIndex(indexId), Duration.Inf).get

    val rangeBuilder = RangeBuilder[K, V](ORDER = TestConfig.MAX_RANGE_ITEMS)(
      DefaultComparators.ordString,
      session,
      global,
      DefaultSerializers.stringSerializer,
      DefaultSerializers.stringSerializer,
      k => k,
      v => v,
      Serializers.grpcRangeCommandSerializer,
      Serializers.grpcMetaCommandSerializer
    )

    val clusterMetaBuilder = IndexBuilder.create[K, KeyIndexContext](DefaultComparators.ordString,
      DefaultSerializers.stringSerializer, Serializers.keyIndexSerializer)
      .storage(storage)
      .cache(cache)
      .serializer(Serializers.grpcStringKeyIndexContextSerializer)
      .valueToStringConverter(Printers.keyIndexContextToStringPrinter)

    val cindex = new ClusterIndex[K, V](metaContext, metaContext.maxNItems)(rangeBuilder, clusterMetaBuilder)

    cindex.inOrder().map{case (k, v, _) => (k, v)}
  }

  def main(args: Array[String]): Unit = {
    val indexIdBefore = s"after-$indexId"

    val ilist = loadAll().toList

    val ks = DefaultSerializers.stringSerializer
    val vs = ks

    val ldata = TestHelper.loadListIndex(indexIdBefore, storage.session).get.data.map { pair =>
      ks.deserialize(pair.key.toByteArray) -> vs.deserialize(pair.value.toByteArray)
    }.toList

    logger.info(s"${Console.GREEN_B}  ldata (ref) len: ${ldata.length}: ${ldata}${Console.RESET}\n")
    logger.info(s"${Console.MAGENTA_B}idata len:       ${ilist.length}: ${ilist}${Console.RESET}\n")

    println("diff: ", ilist.diff(ldata))
    Await.result(storage.close(), Duration.Inf)

    assert(ldata == ilist)
  }

}
