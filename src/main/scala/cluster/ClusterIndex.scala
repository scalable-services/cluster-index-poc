package cluster

import cluster.grpc.{KeyIndexContext, RangeIndexMeta}
import cluster.helpers.{TestConfig, TestHelper}
import com.google.protobuf.ByteString
import services.scalable.index.Commands.{Insert, Remove, Update}
import services.scalable.index.Errors.IndexError
import services.scalable.index.grpc.{IndexContext, RootRef}
import services.scalable.index.{BatchResult, Commands, Errors, IndexBuilder, InsertionResult, QueryableIndex, RemovalResult, UpdateResult}

import java.util.UUID
import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class ClusterIndex[K, V](val metaContext: IndexContext, val maxNItems: Int)(implicit val rangeBuilder: IndexBuilder[K, V],
                                                        val clusterBuilder: IndexBuilder[K, KeyIndexContext]) {

  //assert(maxNItems >= metaContext.maxNItems, s"maxNItems: ${maxNItems} meta max n items: ${metaContext.maxNItems}")

  private var disposable = false

  import clusterBuilder._

  val meta = new QueryableIndex[K, KeyIndexContext](metaContext)(clusterBuilder)
  var ranges = TrieMap.empty[String, RangeIndex[K, V]]

  def save(): Future[IndexContext] = {
    assert(!disposable, s"The cluster index ${metaContext.id} was already saved! Create another instance to perform new operations!")

    disposable = true

    saveIndexes().flatMap { ok =>
      meta.save()
    }
  }

  def saveIndexes(): Future[Boolean] = {
    Future.sequence(ranges.map { case (id, range) =>
      println(s"saving range[2] ${id} => ${range.index.ctx.currentSnapshot().id}")
      //range.save()

      TestHelper.loadOrCreateIndex(range.index.snapshot()).flatMap { ok =>
        range.save()
      }

    }).map(_.toSeq.length == ranges.size)
  }

  def findPath(k: K): Future[Option[(K, RangeIndex[K, V], String)]] = {
    meta.findPath(k).flatMap {
      case None => Future.successful(None)
      case Some(leaf) =>
        val (_, (key, kctx, vs)) = leaf.findPath(k)

        if (!ranges.isDefinedAt(kctx.rangeId)) {
          println(s"${Console.YELLOW_B}RANGE NOT FOUND...${Console.RESET}")
        }

        ranges.get(kctx.rangeId) match {

          case None => RangeIndex.fromId[K, V](kctx.rangeId)(rangeBuilder).map { range =>
            ranges.put(kctx.rangeId, range)
            Some(key, range, vs)
          }

          case Some(range) => Future.successful(Some(key, range, vs))
        }
    }
  }

  def insertMeta(left: RangeIndex[K, V], version: String): Future[BatchResult] = {
    println(s"insert indexes in meta[1]: left ${left.index.ctx.currentSnapshot().id}")

    left.max.map(_.get).flatMap { max =>
      meta.execute(Seq(Commands.Insert[K, KeyIndexContext](
        metaContext.id,
        Seq(Tuple3(max, KeyIndexContext(ByteString.copyFrom(rangeBuilder.keySerializer.serialize(max)),
          left.index.ctx.currentSnapshot().id, left.index.ctx.currentSnapshot().lastChangeVersion), false)),
        Some(version)
      )))
    }
  }

  def insertMeta(left: RangeIndex[K, V], right: RangeIndex[K, V], last: (K, Option[String]), version: String): Future[BatchResult] = {

    println(s"inserting indexes in meta[2]: left ${left.index.ctx.currentSnapshot().id} right: ${right.index.ctx.currentSnapshot().id}")

    Future.sequence(Seq(left.max.map(_.get), right.max.map(_.get))).flatMap { maxes =>
      val lm = maxes(0)
      val rm = maxes(1)

      meta.execute(Seq(
        Commands.Remove[K, KeyIndexContext](metaContext.id, Seq(last), Some(version)),
        Commands.Insert[K, KeyIndexContext](metaContext.id, Seq(
          Tuple3(lm, KeyIndexContext(ByteString.copyFrom(rangeBuilder.keySerializer.serialize(lm)),
            left.index.ctx.indexId, left.index.ctx.lastChangeVersion), false),
          Tuple3(rm, KeyIndexContext(ByteString.copyFrom(rangeBuilder.keySerializer.serialize(rm)),
            right.index.ctx.indexId, right.index.ctx.lastChangeVersion), false)
        ), Some(version))
      ))
    }
  }

  def removeFromMeta(last: (K, Option[String]), version: String): Future[BatchResult] = {
    println(s"removing from meta: ${last}...")
    //meta.remove(Seq(last))

    meta.execute(Seq(
      Commands.Remove[K, KeyIndexContext](metaContext.id, Seq(last), Some(version))
    ))
  }

  def insertEmpty(data: Seq[Tuple3[K, V, Boolean]], version: String): Future[Int] = {

    val leftN = Math.min(maxNItems, data.length)
    val slice = data.slice(0, leftN)

    // TODO: snapshot after insertion?
    val rangeMeta = IndexContext()
      .withId(UUID.randomUUID.toString)
      .withLastChangeVersion(UUID.randomUUID.toString)
      .withMaxNItems(TestConfig.MAX_RANGE_ITEMS)
      .withLevels(0)
      .withNumElements(0L)
      .withNumLeafItems(TestConfig.NUM_LEAF_ENTRIES)
      .withNumMetaItems(TestConfig.NUM_META_ENTRIES)

    val range = new RangeIndex[K, V](rangeMeta)(rangeBuilder)

    ranges.put(range.index.ctx.currentSnapshot().id, range)

    println(s"inserted range ${range.index.ctx.currentSnapshot().id}...")

    range.execute(Seq(Commands.Insert[K, V](rangeMeta.id, slice, Some(version))), version)
      .flatMap { case (result, hasChanged) =>
        assert(result.success, result.error.get)
        insertMeta(range, version).map(_ => slice.length)
    }
  }

  def insertRange(left: RangeIndex[K, V], list: Seq[Tuple3[K, V, Boolean]], last: (K, Option[String]),
                  version: String): Future[Int] = {

    val lindex = left.copy(true)

    val remaining = (lindex.index.ctx.maxNItems - lindex.index.ctx.num_elements).toInt
    val n = Math.min(remaining, list.length)
    val slice = list.slice(0, n)

    assert(remaining >= 0)

    if (remaining == 0) {
      return lindex.split().flatMap { rindex =>
        println(s"${Console.CYAN_B}splitting index ${lindex.index.ctx.currentSnapshot().id}... ${Console.RESET}")

        ranges.put(lindex.index.ctx.indexId, lindex)
        ranges.put(rindex.index.ctx.indexId, rindex)

        println(s"inserted index ${rindex.index.ctx.indexId} with left being: ${lindex.index.ctx.indexId}")
        println(s"ranges1: ${ranges.map(_._2.index.ctx.currentSnapshot().id)}")
        println(s"ranges2: ${ranges.map(_._2.index.ctx.indexId)}")

        insertMeta(lindex, rindex, last, version).map(_ => 0)
      }
    }

    println(s"insert normally ", slice.map { x => rangeBuilder.ks(x._1) })

    //ranges.put(lindex.meta.id, lindex)

    lindex.execute(Seq(Commands.Insert[K, V](metaContext.id, slice, Some(version))), version)
      .flatMap { case (ir, hasChanged) =>
        assert(ir.success, ir.error.get)

        // Update range index on disk
        ranges.put(lindex.index.ctx.indexId, lindex)

        if (hasChanged) {

          lindex.max.map(_.get).flatMap { lm =>
            meta.execute(Seq(
              Commands.Remove[K, KeyIndexContext](metaContext.id, Seq(last), Some(version)),
              Commands.Insert[K, KeyIndexContext](metaContext.id, Seq(
                Tuple3(lm, KeyIndexContext(ByteString.copyFrom(rangeBuilder.keySerializer.serialize(lm)),
                  lindex.index.ctx.indexId, lindex.index.ctx.lastChangeVersion), false)
              ), Some(version))
            )).map { _ => slice.length }
          }

        } else {
          Future.successful(slice.length)
        }
    }
  }

  def insert(data: Seq[Tuple3[K, V, Boolean]], version: String)(implicit ord: Ordering[K]): Future[InsertionResult] = {
    val sorted = data.sortBy(_._1)

    if (sorted.exists { case (k, _, _) => sorted.count { case (k1, _, _) => ord.equiv(k, k1) } > 1 }) {
      return Future.successful(InsertionResult(false, 0, Some(Errors.DUPLICATED_KEYS(data.map(_._1), ks))))
    }

    val len = sorted.length
    var pos = 0

    def insert(): Future[Int] = {
      if (pos == len) return Future.successful(sorted.length)

      var list = sorted.slice(pos, len)
      val (k, _, _) = list(0)

      findPath(k).flatMap {
        case None =>

          insertEmpty(list, version).map { n =>
            println("meta n: ", meta.ctx.num_elements)
            n
          }
        case Some((last, index, vs)) =>

          val idx = list.indexWhere { case (k, _, _) => ord.gt(k, last) }
          if (idx > 0) list = list.slice(0, idx)

          insertRange(index, list, (last, Some(vs)), version)
      }.flatMap { n =>
        println(s"\ninserted: ${n}\n")
        pos += n
        insert()
      }
    }

    insert().map { n =>
      InsertionResult(true, n)
    }.recover {
      case t: IndexError =>
        t.printStackTrace()
        InsertionResult(false, 0, Some(t))
      case t: Throwable =>
        t.printStackTrace()
        throw t
    }
  }

  def update(data: Seq[Tuple3[K, V, Option[String]]], version: String)(implicit ord: Ordering[K]): Future[UpdateResult] = {

    val sorted = data.sortBy(_._1)

    if (sorted.exists { case (k, _, _) => sorted.count { case (k1, _, _) => ord.equiv(k, k1) } > 1 }) {
      return Future.successful(UpdateResult(false, 0, Some(Errors.DUPLICATED_KEYS(sorted.map(_._1), ks))))
    }

    val len = sorted.length
    var pos = 0

    def update(): Future[Int] = {
      if (len == pos) return Future.successful(sorted.length)

      var list = sorted.slice(pos, len)
      val (k, _, _) = list(0)

      findPath(k).flatMap {
        case None => Future.failed(Errors.KEY_NOT_FOUND(k, ks))
        case Some((last, range, vs)) =>

          val idx = list.indexWhere { case (k, _, _) => ord.gt(k, last) }
          if (idx > 0) list = list.slice(0, idx)

          val copy = range.copy(true)

          copy.execute(Seq(Commands.Update[K, V](metaContext.id, list, Some(version))), version).map { case (r, _) =>
            assert(r.success, r.error.get)

            (r, copy, list.length)
          }
      }.flatMap { case (res, copy, n) =>
        if (!res.success) {
          throw res.error.get
        } else {

          ranges.update(copy.index.ctx.currentSnapshot().id, copy)

          pos += n
          update()
        }
      }
    }

    update().map { n =>
      UpdateResult(true, n)
    }.recover {
      case t: IndexError =>
        t.printStackTrace()
        UpdateResult(false, 0, Some(t))
      case t: Throwable =>
        t.printStackTrace()
        throw t
    }
  }

  def remove(data: Seq[Tuple2[K, Option[String]]], version: String)(implicit ord: Ordering[K]): Future[RemovalResult] = {

    val sorted = data.sortBy(_._1)

    if (sorted.exists { case (k, _) => sorted.count { case (k1, _) => ord.equiv(k, k1) } > 1 }) {
      return Future.successful(RemovalResult(false, 0, Some(Errors.DUPLICATED_KEYS(sorted.map(_._1), ks))))
    }

    val len = sorted.length
    var pos = 0

    def remove(): Future[Int] = {
      if (len == pos) return Future.successful(sorted.length)

      var list = sorted.slice(pos, len)
      val (k, _) = list(0)

      findPath(k).flatMap {
        case None => Future.failed(Errors.KEY_NOT_FOUND(k, ks))
        case Some((last, range, vs)) =>

          val idx = list.indexWhere { case (k, _) => ord.gt(k, last) }
          if (idx > 0) list = list.slice(0, idx)

          val copy = range.copy(true)

          copy.execute(Seq(Commands.Remove[K, V](metaContext.id, list)), version)
            .flatMap { case (r, hasChanged) =>

              assert(r.success, r.error.get)

              if (!hasChanged) {
                Future.successful((r, copy, list.length))
              } else if(copy.isEmpty()) {
                removeFromMeta(last -> Some(vs), version).map { res =>
                  (r, copy, list.length)
                }
              } else {
                copy.max.map(_.get).flatMap { cmax =>

                  meta.execute(Seq(
                    Commands.Remove[K, KeyIndexContext](metaContext.id, Seq(last -> Some(vs)), Some(version)),
                    Commands.Insert[K, KeyIndexContext](metaContext.id, Seq(
                      Tuple3(cmax, KeyIndexContext(ByteString.copyFrom(rangeBuilder.keySerializer.serialize(cmax)),
                        copy.index.ctx.currentSnapshot().id, copy.index.ctx.currentSnapshot().lastChangeVersion), false)
                    ), Some(version))
                  )).map { r =>
                    (r, copy, list.length)
                  }

                }
              }

          }

      }.flatMap { case (res, copy, n) =>

        if (!res.success) {
          throw res.error.get
        } else {
          ranges.put(copy.index.ctx.currentSnapshot().id, copy)

          pos += n
          remove()
        }
      }
    }

    remove().map { n =>
      RemovalResult(true, n)
    }.recover {
      case t: IndexError =>
        t.printStackTrace()
        RemovalResult(false, 0, Some(t))
      case t: Throwable =>
        t.printStackTrace()
        throw t
    }
  }

  def execute(cmds: Seq[Commands.Command[K, V]], version: String): Future[BatchResult] = {

    def process(pos: Int, error: Option[Throwable]): Future[BatchResult] = {
      if (error.isDefined) {
        return Future.successful(BatchResult(false, error))
      }

      if (pos == cmds.length) {
        return Future.successful(BatchResult(true))
      }

      val cmd = cmds(pos)

      (cmd match {
        case cmd: Insert[K, V] => insert(cmd.list, version)
        case cmd: Update[K, V] => update(cmd.list, version)
        case cmd: Remove[K, V] => remove(cmd.keys, version)
      }).flatMap(prev => process(pos + 1, prev.error))
    }

    process(0, None)
  }

  def inOrder(): Seq[(K, V, String)] = {
    val iter = Await.result(TestHelper.all(meta.inOrder()), Duration.Inf)

    println(s"${Console.CYAN_B}meta keys: ${iter.map(x => rangeBuilder.ks(x._1))}${Console.RESET}")

    iter.map { case (k, link, version) =>
      val range = ranges.get(link.rangeId) match {
        case None =>

          println(s"try to load the range: ${link.rangeId}...")

          val ctx = Await.result(storage.loadIndex(link.rangeId), Duration.Inf).get
          new RangeIndex[K, V](ctx)(rangeBuilder)

        case Some(range) => range
      }

      println(s"${k} => ${range.inOrder().map(_._1)}")

      range.inOrder()
    }.flatten
  }

}

object ClusterIndex {

  def fromRangeIndexId[K, V](rangeId: String, maxNItems: Int)(implicit rangeBuilder: IndexBuilder[K, V],
                                               clusterBuilder: IndexBuilder[K, KeyIndexContext]): Future[ClusterIndex[K, V]] = {
    import rangeBuilder._

    val metaCtx = IndexContext()
      .withId(UUID.randomUUID.toString)
      .withMaxNItems(Int.MaxValue)
      .withLevels(0)
      .withNumLeafItems(Int.MaxValue)
      .withNumMetaItems(Int.MaxValue)

    def construct(rangeCtx: IndexContext): Future[ClusterIndex[K, V]] = {
      val rangeIndex = new RangeIndex[K, V](rangeCtx)(rangeBuilder)

      val cindex = new ClusterIndex[K, V](metaCtx, maxNItems)

      rangeIndex.max.map(_.get).flatMap { max =>

        cindex.ranges.put(rangeId, rangeIndex)

        val version = TestConfig.TX_VERSION

        cindex.meta.execute(Seq(
          Commands.Insert[K, KeyIndexContext](cindex.metaContext.id, Seq(
            Tuple3(max, KeyIndexContext(ByteString.copyFrom(rangeBuilder.keySerializer.serialize(max)),
              rangeIndex.index.ctx.currentSnapshot().id, rangeIndex.index.ctx.currentSnapshot().lastChangeVersion), false)
          ), Some(version))
        )).map { _ =>
          cindex
        }

      }

    }

    storage.loadIndex(rangeId).map(_.get).flatMap(construct)
  }

}
