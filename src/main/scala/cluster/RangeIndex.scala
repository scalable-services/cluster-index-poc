package cluster

import cluster.grpc.RangeIndexMeta
import com.datastax.oss.driver.api.core.CqlSession
import com.google.protobuf.ByteString
import helpers.TestHelper
import services.scalable.index.Commands.{Command, Insert, Remove, Update}
import services.scalable.index.grpc.KVPair
import services.scalable.index.{BatchResult, Errors, InsertionResult, RemovalResult, Serializer, UpdateResult}

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class RangeIndex[K, V](var meta: RangeIndexMeta)(implicit val ordering: Ordering[K],
                                               val session: CqlSession,
                                               val ec: ExecutionContext,
                                               val ks: Serializer[K],
                                               val vs: Serializer[V],
                                               val kts: K => String,
                                               val vts: V => String) {
  val ctxId = UUID.randomUUID.toString

  var tuples = meta.data.map { p =>
    val key = ks.deserialize(p.key.toByteArray)
    val value = vs.deserialize(p.value.toByteArray)

    Tuple3(key, value, p.version)
  }

  def insert(data: Seq[(K, V, Boolean)]): InsertionResult = {
    if (isFull()) return InsertionResult(false, 0, Some(Errors.LEAF_BLOCK_FULL))

    val n = Math.min(meta.mAX - tuples.length, data.length)
    val slice = data.slice(0, n)

    val len = slice.length

    if (slice.exists { case (k, _, upsert) => tuples.exists { case (k1, _, _) => !upsert && ordering.equiv(k1, k) } }) {
      return InsertionResult(false, 0, Some(Errors.LEAF_DUPLICATE_KEY(slice.map(_._1), kts)))
    }

    // Filter out upsert keys...
    val upserts = slice.filter(_._3)
    tuples = tuples.filterNot { case (k, v, _) => upserts.exists { case (k1, _, _) => ordering.equiv(k, k1) } }

    // Add back the upsert keys and the new ones...
    tuples = (tuples ++ slice.map { case (k, v, _) => Tuple3(k, v, ctxId) }).sortBy(_._1)

    InsertionResult(true, len, None)
  }

  def update(data: Seq[Tuple3[K, V, Option[String]]]):UpdateResult = {
    if (data.exists { case (k, _, _) => !tuples.exists { case (k1, _, _) => ordering.equiv(k1, k) } }) {
      return UpdateResult(false, 0, Some(Errors.LEAF_KEY_NOT_FOUND(data.map(_._1), kts)))
    }

    val versionsChanged = data.filter(_._3.isDefined)
      .filter { case (k0, _, vs0) => tuples.exists { case (k1, _, vs1) => ordering.equiv(k0, k1) && !vs0.get.equals(vs1) } }

    if (!versionsChanged.isEmpty) {
      return UpdateResult(false, 0, Some(Errors.VERSION_CHANGED(versionsChanged.map { case (k, _, vs) => k -> vs }, kts)))
    }

    val notin = tuples.filterNot { case (k1, _, _) => data.exists { case (k, _, _) => ordering.equiv(k, k1) } }

    tuples = (notin ++ data.map { case (k, v, _) => Tuple3(k, v, ctxId) }).sortBy(_._1)

    UpdateResult(true, data.length, None)
  }

  def remove(keys: Seq[Tuple2[K, Option[String]]]): RemovalResult = {
    if (keys.exists { case (k, _) => !tuples.exists { case (k1, _, _) => ordering.equiv(k1, k) } }) {
      return RemovalResult(false, 0, Some(Errors.LEAF_KEY_NOT_FOUND[K](keys.map(_._1), kts)))
    }

    val versionsChanged = keys.filter(_._2.isDefined)
      .filter { case (k0, vs0) => tuples.exists { case (k1, _, vs1) => ordering.equiv(k0, k1) && !vs0.get.equals(vs1) } }

    if (!versionsChanged.isEmpty) {
      return RemovalResult(false, 0, Some(Errors.VERSION_CHANGED(versionsChanged, kts)))
    }

    tuples = tuples.filterNot { case (k, _, _) => keys.exists { case (k1, _) => ordering.equiv(k, k1) } }

    RemovalResult(true, keys.length, None)
  }

  def execute(cmds: Seq[Command[K, V]]): BatchResult = {
    val maxBefore: Option[K] = if(isEmpty()) None else Some(max._1)

    for(i<-0 until cmds.length){
      val r = cmds(i) match {
        case cmd: Insert[K, V] => insert(cmd.list)
        case cmd: Update[K, V] => update(cmd.list)
        case cmd: Remove[K, V] => remove(cmd.keys)
      }

      if(!r.success) return BatchResult(false, r.error)
    }

    // Change last version if max has changed...
    val maxNow: Option[K] = if(isEmpty()) None else Some(max._1)

    if(maxBefore != maxNow){
      println(s"${Console.YELLOW_B}changed version for range ${meta.id}...${Console.RESET}")
      meta = meta.withLastChangeVersion(UUID.randomUUID.toString)
    }

    BatchResult(true)
  }

  def isFull(): Boolean = {
    tuples.length == meta.mAX
  }

  def isEmpty(): Boolean = {
    tuples.isEmpty
  }

  def length = tuples.length

  def min = tuples.minBy(_._1)
  def max = tuples.maxBy(_._1)

  def inOrder(): Seq[(K, V, String)] = tuples

  def toStringAll(): Seq[(String, String, String)] = {
    tuples.map { case (k, v, version) =>
      Tuple3(kts(k), vts(v), version)
    }
  }

  def serialize(): RangeIndexMeta = {
    meta
      .withData(
        tuples.map { case (k, v, version) =>
          val kserial = ks.serialize(k)
          val vserial = vs.serialize(v)
          KVPair(ByteString.copyFrom(kserial), ByteString.copyFrom(vserial), version)
        }
      )
  }

  def copy(): RangeIndex[K, V] = {
    val rcrange = RangeIndexMeta()
      .withId(UUID.randomUUID.toString)
      .withOrder(meta.order)
      .withMIN(meta.mIN)
      .withMAX(meta.mAX)

    val copy = new RangeIndex[K, V](rcrange)

    copy.tuples = tuples
    copy
  }

  def split(): RangeIndex[K, V] = {
    val leftTuples = tuples.slice(0, tuples.length/2)
    val rightTuples = tuples.slice(tuples.length/2, tuples.length)

    tuples = leftTuples

    println(s"${Console.YELLOW_B}changed version for range ${meta.id}...${Console.RESET}")

    // Change last change version...
    meta = meta.withLastChangeVersion(UUID.randomUUID.toString)

    val rcrange = RangeIndexMeta()
      .withId(UUID.randomUUID.toString)
      .withOrder(meta.order)
      .withMIN(meta.mIN)
      .withMAX(meta.mAX)
      .withLastChangeVersion(UUID.randomUUID.toString)

    val right = new RangeIndex[K, V](rcrange)
    right.tuples = rightTuples

    right
  }

  def save(): Future[Boolean] = {
    val snap = serialize()
    TestHelper.saveRange(snap)
  }

}
