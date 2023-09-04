package cluster

import cluster.helpers.TestHelper
import services.scalable.index.Commands.Command
import services.scalable.index.grpc.IndexContext
import services.scalable.index.{BatchResult, IndexBuilder, QueryableIndex}

import java.util.UUID
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class RangeIndex[K, V](protected val meta: IndexContext)(implicit val builder: IndexBuilder[K, V]) {
  import builder._

  var index: QueryableIndex[K, V] = new QueryableIndex[K, V](meta)(builder)
  
  def execute(cmds: Seq[Command[K, V]], version: String): Future[(BatchResult, Boolean)] = {
    index.max().flatMap { maxBOpt =>
      val maxBefore = maxBOpt.map(_._1)

      index.execute(cmds, version).flatMap { br =>

        assert(br.success, br.error)

        index.max().map { maxAOpt =>
          val maxAfter = maxAOpt.map(_._1)

          var changed = false

          if(maxAfter.map(k => maxBefore.map(k1 => builder.ord.equiv(k, k1))).exists(x => x.isDefined && !x.get)){
            changed = true
            println(s"${Console.YELLOW_B}changed version for range ${meta.id}...${Console.RESET}")
            index.ctx.lastChangeVersion = UUID.randomUUID.toString
          }

          br -> changed
        }
      }
    }
  }

  def isFull(): Boolean = {
    index.ctx.num_elements >= meta.maxNItems
  }

  def isEmpty(): Boolean = {
    index.isEmpty()
  }

  def length = index.ctx.num_elements

  def min = index.min().map(_.map(_._1))
  def max = index.max().map(_.map(_._1))

  def inOrder(): Seq[(K, V, String)] = Await.result(TestHelper.all(index.inOrder()), Duration.Inf)

  def serialize(): IndexContext = {
    index.snapshot()
  }

  /*
   * TODO: change this
   */
  def copy(sameId: Boolean = false): RangeIndex[K, V] = {
    val copy = index.copy(sameId)

    val ri = new RangeIndex[K, V](copy.ctx.currentSnapshot())(builder)
    ri.index = copy

    ri
  }

  def split(): Future[RangeIndex[K, V]] = {
    index.split().map { right =>

      //index.ctx.lastChangeVersion = UUID.randomUUID.toString
      println(s"${Console.YELLOW_B}changed version for range ${meta.id}...${Console.RESET}")

      val ri = new RangeIndex[K, V](right.currentSnapshot())(right.builder)
      ri.index = right

      ri
    }
  }

  def save(): Future[IndexContext] = {
    index.save()
  }

}

object RangeIndex {
  def fromCtx[K, V](ctx: IndexContext)(implicit builder: IndexBuilder[K, V]): RangeIndex[K, V] = {
    new RangeIndex[K, V](ctx)(builder)
  }

  def fromId[K, V](id: String)(implicit builder: IndexBuilder[K, V]): Future[RangeIndex[K, V]] = {
    import builder._

    for {
      ctx <- builder.storage.loadIndex(id).map(_.get)
      index = new RangeIndex[K, V](ctx)(builder)
    } yield {
      index
    }
  }
}
