package cluster.demo

import akka.actor.ActorSystem
import akka.grpc.GrpcClientSettings
import cluster.grpc.{ClusterClientResponseService, ClusterClientResponseServiceClient, RangeTaskResponse}
import com.typesafe.config.ConfigFactory

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object Client {

  def main(args: Array[String]): Unit = {

    // Boot akka
    val conf =
      ConfigFactory.parseString("akka.http.server.enable-http2 = on").withFallback(ConfigFactory.defaultApplication())
    implicit val sys = ActorSystem("HelloWorldClient", conf)
    implicit val ec = sys.dispatcher

    // Configure the client by code:
    val clientSettings = GrpcClientSettings.connectToServiceAt("127.0.0.1", 2222)
      .withTls(false)

    // Or via application.conf:
    // val clientSettings = GrpcClientSettings.fromConfig(GreeterService.name)

    // Create a client-side stub for the service
    val client: ClusterClientResponseService = ClusterClientResponseServiceClient(clientSettings)

    Await.result(client.respond(RangeTaskResponse("demo")).map { res =>
      println(s"response: ${res}")
    }, Duration.Inf)
  }

}
