package cluster

import cluster.grpc.RangeIndexMeta
import com.datastax.oss.driver.api.core.CqlSession
import helpers.TestHelper
import org.apache.commons.lang3.RandomStringUtils
import org.scalatest.matchers.should.Matchers
import services.scalable.index.Commands.Insert
import services.scalable.index.DefaultComparators

import java.util.UUID
import java.util.concurrent.ThreadLocalRandom
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class RangeIndexSpec extends Repeatable with Matchers {

  override val times: Int = 10

  type K = String
  type V = String

  import services.scalable.index.DefaultComparators._

  val ordering = DefaultComparators.ordString

  import services.scalable.index.DefaultSerializers._

  "operations" should " run successfully" in {

    val rand = ThreadLocalRandom.current()

    implicit val session = TestHelper.getSession()

    val uuid = UUID.randomUUID().toString

    val order = rand.nextInt(4, 1000)
    val min = order/2
    val max = order

    val rangeMeta = RangeIndexMeta()
      .withId(uuid)
      .withOrder(order)
      .withMIN(min)
      .withMAX(max)

    val crange = Await.result(TestHelper.getRange(rangeMeta.id).flatMap {
      case None => TestHelper.createRange(rangeMeta).map(_ => rangeMeta)
      case Some(range) => Future.successful(range)
    }, Duration.Inf)

    val left = new RangeIndex[K, V](crange)

    var data = Seq.empty[(K, V, String)]

    def insert(): Insert[K, V] = {
      var list = Seq.empty[(K, V, Boolean)]
      val n = rand.nextInt(4, max)

      for (i <- 0 until n) {
        val k = RandomStringUtils.randomAlphanumeric(5)
        val v = RandomStringUtils.randomAlphanumeric(5)

        if (!list.exists{case (k1, _, _) => ordering.equiv(k, k1)} && !data.exists { case (k1, _, _) => ordering.equiv(k, k1)}) {
          list :+= Tuple3(k, v, false)
        }
      }

      data = data ++ list.map{case (k, v, _) => Tuple3(k, v, UUID.randomUUID.toString)}

      Insert(left.meta.id, list)
    }

    val insertCommand = insert()
    val executionResult = left.execute(Seq(insertCommand))

    if(executionResult.error.isDefined){
      throw executionResult.error.get
      System.exit(1)
    }

    val saveResult = Await.result(left.save(), Duration.Inf)

    assert(saveResult, "error on saving range to storage...")

    val ctxFromDisk = Await.result(TestHelper.getRange(left.meta.id), Duration.Inf)

    assert(ctxFromDisk.isDefined)

    val leftFromDisk = new RangeIndex[K, V](ctxFromDisk.get)

    val sortedData = data.sortBy(_._1).map { case (k, v, _) => k -> v }.toList
    val rangeData = leftFromDisk.inOrder().map { case (k, v, _) => k -> v }.toList

    println(s"${Console.GREEN_B}refer data: ${sortedData}${Console.RESET}")
    println(s"${Console.MAGENTA_B}index data: ${sortedData}${Console.RESET}")
    println()

    assert(rangeData == sortedData)

    val copy = left.copy()
    val right = copy.split()

    val saveResultsOpts = Await.result(Future.sequence(Seq(
      copy.save(),
      right.save()
    )), Duration.Inf)

    assert(saveResultsOpts.forall(_ == true), "error on saving left and right to storage...")

    val leftHalfCtx = Await.result(TestHelper.getRange(copy.meta.id), Duration.Inf)
    val rightHalfCtx = Await.result(TestHelper.getRange(right.meta.id), Duration.Inf)

    assert(leftHalfCtx.isDefined)
    assert(rightHalfCtx.isDefined)

    val leftHalfFromDisk = new RangeIndex[K, V](leftHalfCtx.get)
    val rightHalfFromDisk = new RangeIndex[K, V](rightHalfCtx.get)

    val all = leftFromDisk.inOrder().map{case (k, v, _) => k -> v}.toList
    val leftInOrder = leftHalfFromDisk.inOrder().map{case (k, v, _) => k -> v}.toList
    val rightInOder = rightHalfFromDisk.inOrder().map{case (k, v, _) => k -> v}.toList

    println(s"${Console.YELLOW_B}left: ", leftInOrder, s"${Console.RESET}")
    println(s"${Console.GREEN_B}right: ", rightInOder, s"${Console.RESET}")
    println(s"${Console.CYAN_B}all: ", all, s"${Console.RESET}")
    println()

    assert((leftInOrder ++ rightInOder) == all)

    Await.result(TestHelper.truncateRanges(), Duration.Inf)
    session.close()
  }

}
