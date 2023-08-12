package cluster

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

object Main {

  def main(args: Array[String]): Unit = {

    MetaWorkerStarter.main(args)
    RangeWorkersStarter.main(args)

    val systems = MetaWorkerStarter.systems ++ RangeWorkersStarter.systems

    Await.result(Future.sequence(systems.map(_.whenTerminated)), Duration.Inf)

  }

}
