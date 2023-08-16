package cluster

import cluster.grpc.KeyIndexContext
import services.scalable.index.Commands

object ClusterCommands {
  trait ClusterCommand[K, V] {
    val id: String
  }

  case class RangeCommand[K, V](override val id: String,
                                rangeId: String,
                                indexId: String,
                                commands: Seq[Commands.Command[K, V]],
                                keyInMeta: Tuple2[K, String],
                                lastChangeVersion: String,
                                responseTopic: String
                               ) extends ClusterCommand[K, V]
  case class MetaCommand[K](override val id: String,
                               metaId: String,
                               commands: Seq[Commands.Command[K, KeyIndexContext]],
                               responseTopic: String
                              ) extends ClusterCommand[K, KeyIndexContext]
}
