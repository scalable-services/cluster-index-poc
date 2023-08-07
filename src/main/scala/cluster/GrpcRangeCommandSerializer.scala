package cluster

import cluster.ClusterCommands.RangeCommand
import cluster.grpc.RangeTask
import com.google.protobuf.ByteString
import services.scalable.index.{Bytes, Commands, Serializer}
import com.google.protobuf.any.Any

final class GrpcRangeCommandSerializer[K, V](implicit val commandsSerializer: Serializer[Commands.Command[K, V]])
  extends Serializer[RangeCommand[K, V]] {
  override def serialize(t: RangeCommand[K, V]): Bytes = {
    Any.pack(RangeTask(
      t.id,
      t.rangeId,
      t.commands.map { c => ByteString.copyFrom(commandsSerializer.serialize(c)) },
      t.lastChangeVersion),
    ).toByteArray
  }

  override def deserialize(b: Bytes): RangeCommand[K, V] = {
    val t = Any.parseFrom(b).unpack(RangeTask)

    RangeCommand(t.id, t.rangeId, t.commands.map { c => commandsSerializer.deserialize(c.toByteArray) },
      t.lastChangeVersion)
  }
}
