import cluster.grpc.KeyIndexContext
import com.datastax.oss.driver.api.core.config.{DefaultDriverOption, DriverConfigLoader}
import com.google.protobuf.any.Any
import services.scalable.index.impl.{GrpcByteSerializer, GrpcCommandSerializer}
import services.scalable.index.{Bytes, Serializer}

package object cluster {

  /*val ORDER = 10
  val MIN = ORDER / 2
  val MAX = ORDER*/

  val KEYSPACE = "history"

  val loader =
    DriverConfigLoader.programmaticBuilder()
      .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, java.time.Duration.ofSeconds(30))
      .withInt(DefaultDriverOption.CONNECTION_MAX_REQUESTS, 31768)
      .withInt(DefaultDriverOption.SESSION_LEAK_THRESHOLD, 1000)
      .withString(DefaultDriverOption.PROTOCOL_VERSION, "V4")
      .withString(DefaultDriverOption.RECONNECTION_POLICY_CLASS, "ExponentialReconnectionPolicy")
      .withDuration(DefaultDriverOption.RECONNECTION_BASE_DELAY, java.time.Duration.ofSeconds(1))
      .withDuration(DefaultDriverOption.RECONNECTION_MAX_DELAY, java.time.Duration.ofSeconds(10))
      /*.startProfile("slow")
      .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(30))
      .endProfile()*/
      .build()

  object Serializers {

    import services.scalable.index.DefaultSerializers._

    implicit val keyIndexSerializer = new Serializer[KeyIndexContext] {
      override def serialize(t: KeyIndexContext): Bytes = Any.pack(t).toByteArray

      override def deserialize(b: Bytes): KeyIndexContext = Any.parseFrom(b).unpack(KeyIndexContext)
    }

    implicit val grpcStringStringSerializer = new GrpcByteSerializer[String, String]()

    implicit val grpcStringKeyIndexContextSerializer = new GrpcByteSerializer[String, KeyIndexContext]()

    implicit val grpcStringStringCommandsSerializer = new GrpcCommandSerializer[String, String]()
    implicit val grpcStringKeyIndexContextCommandsSerializer = new GrpcCommandSerializer[String, KeyIndexContext]()
    implicit val grpcRangeCommandSerializer = new GrpcRangeCommandSerializer[String, String]()
    implicit val grpcMetaCommandSerializer = new GrpcMetaCommandSerializer[String]()
  }

  object Printers {
    implicit def keyIndexContextToStringPrinter(k: KeyIndexContext): String = k.toString
  }

}
