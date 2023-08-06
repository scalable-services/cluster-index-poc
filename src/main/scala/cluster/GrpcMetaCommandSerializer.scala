package cluster

import cluster.ClusterCommands.MetaCommand
import cluster.grpc.{KeyIndexContext, MetaTask}
import com.google.protobuf.ByteString
import com.google.protobuf.any.Any
import services.scalable.index.{Bytes, Commands, Serializer}

final class GrpcMetaCommandSerializer[K](implicit val commandsSerializer: Serializer[Commands.Command[K, KeyIndexContext]])
  extends Serializer[MetaCommand[K]] {
  override def serialize(t: MetaCommand[K]): Bytes = {
    Any.pack(MetaTask(
      t.id,
      t.metaId,
      t.commands.map { c => ByteString.copyFrom(commandsSerializer.serialize(c)) }, t.responseTopic)
    ).toByteArray
  }

  override def deserialize(b: Bytes): MetaCommand[K] = {
    val t = Any.parseFrom(b).unpack(MetaTask)
    MetaCommand(t.id, t.metaId, t.commands.map { c => commandsSerializer.deserialize(c.toByteArray) }, t.responseTopic)
  }
}
