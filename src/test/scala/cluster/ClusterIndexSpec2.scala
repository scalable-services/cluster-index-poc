package cluster

import cluster.grpc.KeyIndexContext
import cluster.helpers.{TestConfig, TestHelper}
import org.apache.commons.lang3.RandomStringUtils
import org.scalatest.matchers.should.Matchers
import services.scalable.index.Commands.Insert
import services.scalable.index.grpc.IndexContext
import services.scalable.index.impl.{CassandraStorage, DefaultCache}
import services.scalable.index.{Commands, DefaultComparators, DefaultIdGenerators, DefaultSerializers, IndexBuilder}

import java.util.concurrent.ThreadLocalRandom
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class ClusterIndexSpec2 extends Repeatable with Matchers {

  override val times: Int = 1

  type K = String
  type V = String

  val ordering = DefaultComparators.ordString

  "operations" should " run successfully" in {

    val indexId = TestConfig.CLUSTER_INDEX_NAME
    val rand = ThreadLocalRandom.current()

    implicit val session = TestHelper.getSession()

    TestHelper.truncateAll()

    val version = TestConfig.TX_VERSION//UUID.randomUUID.toString

    val order = rand.nextInt(4, 1000)
    val min = order / 2
    val max = order

    var data = Seq.empty[(K, V, String)]
    var excludeInsertions = Seq.empty[(K, V, String)]

    implicit val idGenerator = DefaultIdGenerators.idGenerator
    implicit val cache = new DefaultCache(MAX_PARENT_ENTRIES = 20000)
    //implicit val storage = new MemoryStorage()
    implicit val storage = new CassandraStorage(session, true)

    val rangeBuilder = IndexBuilder.create[K, V](
      ordering,
      DefaultSerializers.stringSerializer,
      DefaultSerializers.stringSerializer
    )

    rangeBuilder.storage(storage)
    rangeBuilder.cache(cache)
    rangeBuilder.serializer(Serializers.grpcStringStringSerializer)

    val clusterMetaBuilder = IndexBuilder.create[K, KeyIndexContext](DefaultComparators.ordString,
      DefaultSerializers.stringSerializer, Serializers.keyIndexSerializer)
      .storage(storage)
      .cache(cache)
      .serializer(Serializers.grpcStringKeyIndexContextSerializer)
      .valueToStringConverter(Printers.keyIndexContextToStringPrinter)

    def insert(min: Int = 400, max: Int = 1000): Seq[Insert[K, V]] = {
      var list = Seq.empty[(K, V, Boolean)]
      val n = rand.nextInt(min, max)

      for (i <- 0 until n) {
        val k = RandomStringUtils.randomAlphanumeric(5)
        val v = RandomStringUtils.randomAlphanumeric(5)

        if (!list.exists { case (k1, _, _) => ordering.equiv(k, k1) } && !data.exists { case (k1, _, _) => ordering.equiv(k, k1) }) {
          list :+= Tuple3(k, v, false)
        }
      }

      data = data ++ list.map { case (k, v, _) => Tuple3(k, v, version) }

      Seq(Insert(indexId, list, Some(version)))
    }

    def update(): Seq[Commands.Command[K, V]] = {
      val n = if (data.length >= 2) rand.nextInt(1, data.length) else 1

      val list = scala.util.Random.shuffle(data.filterNot{x => excludeInsertions.exists{y => ordering.equiv(x._1, y._1)}}).slice(0, n)
      if (list.isEmpty) return Seq.empty[Commands.Command[K, V]]

      var updates = Seq.empty[(K, V, Option[String])]

      list.foreach { case (k, v, lv) =>
        val value = RandomStringUtils.randomAlphanumeric(5)
        updates = updates :+ (k, value, Some(lv))
      }

      println(s"update size: ${list.length}")
      println(s"updates: ${updates.map { case (k, v, _) => rangeBuilder.ks(k) -> rangeBuilder.vs(v) }}")

      data = data.filterNot { case (k, _, _) => list.exists { case (k1, _, _) => ordering.equiv(k, k1) } }
      data = data ++ updates.map { case (k, v, lv) => (k, v, lv.get) }

      Seq(Commands.Update(indexId, updates, Some(version)))
    }

    def remove(): Seq[Commands.Command[K, V]] = {
      val time = System.nanoTime()

      val n = (data.length * 0.85).toInt //if (data.length >= 2) rand.nextInt(1, data.length) else 1

      val list = scala.util.Random.shuffle(data.filterNot{x => excludeInsertions.exists{y => ordering.equiv(x._1, y._1)}}).slice(0, n).map { case (k, _, lv) =>
        (k, Some(lv))
      }

      if (list.isEmpty) return Seq.empty[Commands.Command[K, V]]

      println(s"remove size: ${list.length}")
      println(s"removals: ${list.map { case (k, _) => rangeBuilder.ks(k) }}")

      data = data.filterNot { case (k, _, _) => list.exists { case (k1, _) => ordering.equiv(k, k1) } }

      Seq(Commands.Remove[K, V](indexId, list, Some(version)))
    }

    val metaContext = Await.result(TestHelper.loadOrCreateIndex(IndexContext(
      indexId,
      TestConfig.NUM_LEAF_ENTRIES,
      TestConfig.NUM_META_ENTRIES
    ).withMaxNItems(Int.MaxValue)), Duration.Inf).get

    val cindex = new ClusterIndex[K, V](metaContext, TestConfig.MAX_RANGE_ITEMS)(rangeBuilder, clusterMetaBuilder)

    var commands: Seq[Commands.Command[K, V]] = insert(2000, 3000)
    val ctx = Await.result(cindex.execute(commands, version).flatMap(_ => cindex.save()), Duration.Inf)

    val listIndex = data.sortBy(_._1)
    println(s"${Console.YELLOW_B}listindex after range cmds inserted: ${TestHelper.saveListIndex(indexId, listIndex, storage.session, rangeBuilder.keySerializer, rangeBuilder.valueSerializer)}${Console.RESET}")

    def compare(): Unit = {
      val indexDataFromDisk = LoadIndexDemo.loadAll().toList
      val listDataFromDisk = LoadIndexDemo.loadListIndex(indexId).toList

      assert(indexDataFromDisk == listDataFromDisk)

      println(s"${Console.GREEN_B}  ldata (ref) len: ${indexDataFromDisk.length}: ${indexDataFromDisk}${Console.RESET}\n")
      println(s"${Console.MAGENTA_B}idata len:       ${listDataFromDisk.length}: ${listDataFromDisk}${Console.RESET}\n")

      println("diff: ", listDataFromDisk.diff(indexDataFromDisk))
    }

    compare()

    Await.result(storage.close(), Duration.Inf)
    session.close()
  }

}
