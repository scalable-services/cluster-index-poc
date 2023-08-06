package cluster

import cluster.grpc.KeyIndexContext
import services.scalable.index.Commands

object ClusterCommands {
  trait ClusterCommand[K, V] {
    val id: String
  }

  case class RangeCommand[K, V](override val id: String,
                                rangeId: String,
                                commands: Seq[Commands.Command[K, V]],
                                lastChangeVersion: String) extends ClusterCommand[K, V]
  case class MetaCommand[K](override val id: String,
                               metaId: String,
                               commands: Seq[Commands.Command[K, KeyIndexContext]],
                               responseTopic: String
                              ) extends ClusterCommand[K, KeyIndexContext]
}
