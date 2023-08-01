package cluster

import cluster.grpc.KeyIndexContext
import com.google.protobuf.ByteString
import helpers.{TestConfig, TestHelper}
import org.apache.commons.lang3.RandomStringUtils
import org.scalatest.matchers.should.Matchers
import services.scalable.index.Commands.Insert
import services.scalable.index.grpc.IndexContext
import services.scalable.index.impl.{CassandraStorage, DefaultCache}
import services.scalable.index.{Commands, DefaultComparators, DefaultIdGenerators, DefaultSerializers, IndexBuilder, QueryableIndex}

import java.util.UUID
import java.util.concurrent.ThreadLocalRandom
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

class ClusterIndexSpec extends Repeatable with Matchers {

  override val times: Int = 1

  type K = String
  type V = String

  val ordering = DefaultComparators.ordString

  "operations" should " run successfully" in {

    val indexId = TestConfig.CLUSTER_INDEX_NAME
    val rand = ThreadLocalRandom.current()

    implicit val session = TestHelper.getSession()

    TestHelper.truncateAll()

    val order = rand.nextInt(4, 1000)
    val min = order / 2
    val max = order

    var data = Seq.empty[(K, V, String)]

    implicit val idGenerator = DefaultIdGenerators.idGenerator
    implicit val cache = new DefaultCache(MAX_PARENT_ENTRIES = 20000)
    //implicit val storage = new MemoryStorage()
    implicit val storage = new CassandraStorage(session, true)

    val rangeBuilder = RangeBuilder[K, V](ORDER = 64)(
      ordering,
      session,
      global,
      DefaultSerializers.stringSerializer,
      DefaultSerializers.stringSerializer,
      k => k,
      v => v
    )

    val clusterMetaBuilder = IndexBuilder.create[K, KeyIndexContext](DefaultComparators.ordString,
      DefaultSerializers.stringSerializer, Serializers.keyIndexSerializer)
      .storage(storage)
      .cache(cache)
      .serializer(Serializers.grpcStringKeyIndexContextSerializer)
      .valueToStringConverter(Printers.keyIndexContextToStringPrinter)

    def insert(): Seq[Insert[K, V]] = {
      var list = Seq.empty[(K, V, Boolean)]
      val n = rand.nextInt(400, 1000)

      for (i <- 0 until n) {
        val k = RandomStringUtils.randomAlphanumeric(5)
        val v = RandomStringUtils.randomAlphanumeric(5)

        if (!list.exists { case (k1, _, _) => ordering.equiv(k, k1) } && !data.exists { case (k1, _, _) => ordering.equiv(k, k1) }) {
          list :+= Tuple3(k, v, false)
        }
      }

      data = data ++ list.map { case (k, v, _) => Tuple3(k, v, UUID.randomUUID.toString) }

      Seq(Insert(indexId, list))
    }

    def update(): Seq[Commands.Command[K, V]] = {
      val n = if (data.length >= 2) rand.nextInt(1, data.length) else 1

      val list = scala.util.Random.shuffle(data).slice(0, n)
      if (list.isEmpty) return Seq.empty[Commands.Command[K, V]]

      var updates = Seq.empty[(K, V, Option[String])]

      list.foreach { case (k, v, lv) =>
        val value = RandomStringUtils.randomAlphanumeric(5)
        updates = updates :+ (k, value, Some(lv))
      }

      println(s"update size: ${list.length}")
      println(s"updates: ${updates.map { case (k, v, _) => rangeBuilder.kts(k) -> rangeBuilder.vts(v) }}")

      data = data.filterNot { case (k, _, _) => list.exists { case (k1, _, _) => ordering.equiv(k, k1) } }
      data = data ++ updates.map { case (k, v, lv) => (k, v, lv.get) }

      Seq(Commands.Update(indexId, updates))
    }

    def remove(): Seq[Commands.Command[K, V]] = {
      val time = System.nanoTime()

      val n = if (data.length >= 2) rand.nextInt(1, data.length) else 1

      val list = scala.util.Random.shuffle(data).slice(0, n).map { case (k, _, lv) =>
        (k, Some(lv))
      }

      if (list.isEmpty) return Seq.empty[Commands.Command[K, V]]

      println(s"remove size: ${list.length}")
      println(s"removals: ${list.map { case (k, _) => rangeBuilder.kts(k) }}")

      data = data.filterNot { case (k, _, _) => list.exists { case (k1, _) => ordering.equiv(k, k1) } }

      Seq(Commands.Remove[K, V](indexId, list))
    }

    val metaContext = Await.result(TestHelper.loadOrCreateIndex(IndexContext(
      indexId,
      TestConfig.NUM_LEAF_ENTRIES,
      TestConfig.NUM_META_ENTRIES
    ).withMaxNItems(Int.MaxValue)), Duration.Inf).get

    val cindex = new ClusterIndex[K, V](metaContext, TestConfig.MAX_RANGE_ITEMS)(rangeBuilder, clusterMetaBuilder)

    val version = UUID.randomUUID.toString

    var commands: Seq[Commands.Command[K, V]] = insert()
    val br = Await.result(cindex.execute(commands, version)/*.flatMap(_ => cindex.save())*/, Duration.Inf)

    var dataSorted = data.sortBy(_._1).toList
    var rangeData = cindex.inOrder().toList

    println(s"${Console.GREEN_B}ref data: ${dataSorted.map{case (k, v, _) => k -> v}}${Console.RESET}")
    println(s"${Console.YELLOW_B}range data: ${rangeData.map{case (k, v, _) => k -> v}}${Console.RESET}")

    assert(rangeData.map{case (k, v, _) => k -> v} == dataSorted.map{case (k, v, _) => k -> v})

    /*val metaContext2 = Await.result(TestHelper.loadIndex(metaContext.id), Duration.Inf).get
    val cindex2 = new ClusterIndex[K, V](metaContext2, TestConfig.MAX_RANGE_ITEMS)(rangeBuilder, clusterMetaBuilder)*/

    data = rangeData
    val updateCommands = update()
    val removalCommands = remove()

    commands = updateCommands ++ removalCommands
    val br2 = Await.result(cindex.execute(commands, version).flatMap(_ => cindex.save()), Duration.Inf)

    dataSorted = data.sortBy(_._1).toList
    rangeData = cindex.inOrder().toList

    println(s"${Console.GREEN_B}ref data: ${dataSorted.map { case (k, v, _) => k -> v }}${Console.RESET}")
    println(s"${Console.YELLOW_B}range data: ${rangeData.map { case (k, v, _) => k -> v }}${Console.RESET}")

    assert(rangeData.map{case (k, v, _) => k -> v} == dataSorted.map{case (k, v, _) => k -> v})

    Await.result(storage.close(), Duration.Inf)
    session.close()
  }

}
