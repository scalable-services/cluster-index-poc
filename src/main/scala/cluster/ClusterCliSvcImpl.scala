package cluster

import akka.stream.Materializer
import cluster.grpc.{ClusterClientResponseService, RangeTaskResponse}

import scala.concurrent.Future

abstract class ClusterCliSvcImpl(implicit mat: Materializer) extends ClusterClientResponseService {
  import mat.executionContext
}
