package cluster

import cluster.grpc.KeyIndexContext
import services.scalable.index.IndexBuilder
import services.scalable.index.grpc.IndexContext

class ClusterIndex[K, V](val metaContext: IndexContext)(val clusterBuilder: IndexBuilder[K, KeyIndexContext]) {

 /* import clusterBuilder._

  val meta = new QueryableIndex[K, KeyIndexContext](metaContext)(clusterBuilder)
  var ranges = TrieMap.empty[String, ClusterRange]

  def findPath(k: K): Future[Option[(K, ClusterRange, String)]] = {
    meta.findPath(k).flatMap {
      case None => Future.successful(None)
      case Some(leaf) =>
        val (_, (key, kctx, vs)) = leaf.findPath(k)

        if (!ranges.isDefinedAt(kctx.rangeId)) {
          println(s"${Console.YELLOW_B}RANGE NOT DEFINED...${Console.RESET}")
        }

        ranges.get(kctx.rangeId) match {
          case None => TestHelper.getRange(kctx.rangeId)(session, ec).map { r =>
            ranges.put(kctx.rangeId, r.get)
            Some(key, r.get, vs)
          }

          case Some(range) => Future.successful(Some(key, range, vs))
        }
    }
  }

  def insertMeta(left: ClusterRange): Future[InsertionResult] = {
    println(s"insert indexes in meta[1]: left ${left.id}")

    left.max().flatMap { lm =>
      meta.insert(Seq(Tuple3(lm.get._1, KeyIndexContext(ByteString.copyFrom(indexBuilder.keySerializer.serialize(lm.get._1)),
        left.ctx.indexId, left.ctx.id), true)))
    }
  }

  def insertEmpty(data: Seq[Tuple3[K, V, Boolean]]): Future[Int] = {

    val leftN = Math.min(maxNItems, data.length)
    val slice = data.slice(0, leftN)

    val range = ClusterRange()

    ranges.put(range.id, range)

    println(s"inserted range ${range.id}...")

    null
  }

  def insert(data: Seq[Tuple3[K, V, Boolean]])(implicit ord: Ordering[K]): Future[InsertionResult] = {
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

          insertEmpty(list).map { n =>
            println("meta n: ", meta.ctx.num_elements)
            n
          }
        case Some((last, index, vs)) =>

          val idx = list.indexWhere { case (k, _, _) => ord.gt(k, last) }
          if (idx > 0) list = list.slice(0, idx)

          insertRange(index, list, (last, Some(vs)))
      }.flatMap { n =>
        println(s"\ninserted: ${n}\n")
        pos += n
        insert()
      }
    }

    insert().map { n =>
      InsertionResult(true, n)
    }.recover {
      case t: IndexError => InsertionResult(false, 0, Some(t))
      case t: Throwable => throw t
    }
  }*/

}
