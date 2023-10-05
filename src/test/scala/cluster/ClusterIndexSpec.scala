package cluster

import cluster.ClusterCommands.RangeCommand
import cluster.grpc.KeyIndexContext
import cluster.helpers.{TestConfig, TestHelper}
import org.apache.commons.lang3.RandomStringUtils
import org.scalatest.matchers.should.Matchers
import services.scalable.index.Commands.Insert
import services.scalable.index.grpc.IndexContext
import services.scalable.index.impl.{CassandraStorage, DefaultCache}
import services.scalable.index.{Commands, DefaultComparators, DefaultIdGenerators, DefaultSerializers, IndexBuilder}

import java.util.concurrent.ThreadLocalRandom
import scala.concurrent.{Await, Future}
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

    val version = TestConfig.TX_VERSION//UUID.randomUUID.toString

    val order = 32//rand.nextInt(4, 1000)
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

    var commands: Seq[Commands.Command[K, V]] = insert(1500, 3000)
    val ctx = Await.result(cindex.execute(commands, version).flatMap(_ => cindex.save()), Duration.Inf)

    var dataSorted = data.sortBy(_._1).toList
    var rangeData = cindex.inOrder().toList

    println(s"${Console.GREEN_B}ref data: ${dataSorted.map{case (k, v, _) => k -> v}}${Console.RESET}")
    println(s"${Console.YELLOW_B}range data: ${rangeData.map{case (k, v, _) => k -> v}}${Console.RESET}")

    assert(rangeData.map{case (k, v, _) => k -> v} == dataSorted.map{case (k, v, _) => k -> v})

    val nGroups = 3

    commands = Seq.empty[Commands.Command[K, V]]
    //val grouped = data.grouped(nGroups).toSeq

    var tasks = Seq.empty[Future[Boolean]]

    var list = Seq.empty[(K, V, String)]
    //var list = Seq.empty[(K, V, String)]

    for(i<-0 until rand.nextInt(1000, 3000)){
      val k = RandomStringUtils.randomAlphanumeric(6)
      val v = RandomStringUtils.randomAlphanumeric(6)

      if(!list.exists{case (k1, _, _) => ordering.equiv(k, k1)} && !data.exists{case (k1, _, _) => ordering.equiv(k, k1)}){
        list = list :+ (k, v, version)
      }
    }

    val groupedNewInsertions = list.grouped(nGroups).toSeq
    val grouped = data.grouped(nGroups).toSeq

    println(s"\n${Console.YELLOW_B}EXECUTING ${grouped.length} TXS...${Console.RESET}\n")
    //Thread.sleep(2000)

    var pool = Seq.empty[ClusterClient[K, V]]

    for(i<-0 until 10){
      val client = new ClusterClient[K, V](ctx)(clusterMetaBuilder, session, Serializers.grpcRangeCommandSerializer)
      pool = pool :+ client
    }

    Await.result(Future.sequence(pool.map(_.start())), Duration.Inf)

    for(i<-0 until groupedNewInsertions.length){

      val groupedInsertion = groupedNewInsertions(i)
      val groupData = grouped(i)
      //val groupedOps = groupData.grouped(10).toSeq

      val updates = groupData.slice(0, groupData.length/2).map { case (k, v, vs) =>
        Tuple3(k, RandomStringUtils.randomAlphanumeric(6), Some(vs))
      }

      val removals = groupData.slice(groupData.length/2, groupData.length).map { case (k, v, vs) => (k, Some(vs)) }

      val insert = Commands.Insert[K, V](metaContext.id, groupedInsertion.map { case (k, v, _) => (k, v, false) },
        Some(version))
      val update = Commands.Update[K, V](metaContext.id, updates, Some(version))
      val removal = Commands.Remove[K, V](metaContext.id, removals, Some(version))

      data = data.filterNot { case (k, _, _) =>
        updates.exists { case (k1, _, _) => ordering.equiv(k, k1) }
      }

      data = data ++ updates.map { case (k, v, vs) => (k, v, version) }

      data = data.filterNot { case (k, _, _) =>
        removals.exists { case (k1, _) => ordering.equiv(k, k1) }
      }

      data = data ++ groupedInsertion

      val cmds: Seq[Commands.Command[K, V]] = Seq(update, removal, insert)
      val client = pool(rand.nextInt(0, pool.length))

      println(s"removals: ${removal.keys.length} updates: ${update.list.length} insertions: ${insert.list.length}")

      tasks = tasks :+ client.execute(cmds).flatMap { mcmds =>
        client.sendTasks(mcmds.values.toSeq)
      }
    }

    println(s"result of ${tasks.length} tasks: ", Await.result(Future.sequence(tasks), Duration.Inf))

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

    Await.result(Future.sequence(pool.map(_.close())).flatMap(_ => storage.close()), Duration.Inf)
    session.close()
  }

}
