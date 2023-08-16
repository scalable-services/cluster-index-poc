package cluster.demo

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.stream.Materializer
import cluster.ClusterCliSvcImpl
import cluster.grpc.{ClusterClientResponseService, ClusterClientResponseServiceHandler, HelloReply, RangeTaskResponse}
import com.typesafe.config.ConfigFactory

import scala.concurrent.Future

object Server {

  def main(args: Array[String]): Unit = {

    val conf =
      ConfigFactory.parseString("akka.http.server.enable-http2 = on").withFallback(ConfigFactory.defaultApplication())
    implicit val system = ActorSystem("HelloWorld", conf)

    // Akka boot up code
    //implicit val sys: ActorSystem = system
    // implicit val ec: ExecutionContext = system.dispatcher
    implicit val mat = Materializer(system)
    implicit val ec = system.dispatcher

    //val system = ActorSystem(s"client-${client_uuid}", conf)

    // Create service handlers
    val service: HttpRequest => Future[HttpResponse] =
      ClusterClientResponseServiceHandler(new ClusterClientResponseService() {
        override def respond(r: RangeTaskResponse): Future[HelloReply] = {

          Future.successful(HelloReply(r.id, r.ok))
        }
      })

    // Bind service handler servers to localhost:8080/8081
    val binding = Http().newServerAt("127.0.0.1", 2222).bind(service)

    // report successful binding
    binding.foreach { binding =>
      println(s"gRPC server bound to: ${binding.localAddress} at port ${binding.localAddress.getPort}")
    }

  }

}
