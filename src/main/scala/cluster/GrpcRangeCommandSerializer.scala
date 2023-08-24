package cluster

import cluster.ClusterCommands.RangeCommand
import cluster.grpc.RangeTask
import com.google.protobuf.ByteString
import services.scalable.index.{Bytes, Commands, Serializer}
import com.google.protobuf.any.Any
import services.scalable.index.grpc.KVPair

final class GrpcRangeCommandSerializer[K, V](implicit val commandsSerializer: Serializer[Commands.Command[K, V]],
                                             val ks: Serializer[K])
  extends Serializer[RangeCommand[K, V]] {
  override def serialize(t: RangeCommand[K, V]): Bytes = {
    Any.pack(RangeTask(
      t.id,
      t.rangeId,
      t.commands.map { c => ByteString.copyFrom(commandsSerializer.serialize(c)) },
      KVPair(ByteString.copyFrom(ks.serialize(t.keyInMeta._1)), ByteString.EMPTY, t.keyInMeta._2),
      t.lastChangeVersion,
      t.responseTopic,
      t.indexId
    )).toByteArray
  }

  override def deserialize(b: Bytes): RangeCommand[K, V] = {
    val t = Any.parseFrom(b).unpack(RangeTask)

    RangeCommand(t.id, t.rangeId, t.indexId, t.commands.map { c => commandsSerializer.deserialize(c.toByteArray) },
      Tuple2(ks.deserialize(t.keyInMeta.key.toByteArray), t.keyInMeta.version), t.lastChangeVersion, t.responseTopic)
  }
}
