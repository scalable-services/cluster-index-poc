package cluster

import akka.actor.ActorSystem
import akka.kafka._
import akka.kafka.scaladsl.{Committer, Consumer, Producer}
import akka.stream.scaladsl.{Sink, Source}
import cluster.grpc.{KeyIndexContext, MetaTask, MetaTaskResponse}
import cluster.helpers.{TestConfig, TestHelper}
import com.google.protobuf.any.Any
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer, StringDeserializer, StringSerializer}
import org.slf4j.LoggerFactory
import services.scalable.index.{Bytes, IndexBuilder, QueryableIndex}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class MetaWorker[K, V](val id: String)(implicit val indexBuilder: IndexBuilder[K, KeyIndexContext],
                                       val metaTaskSerializer: GrpcMetaCommandSerializer[K]) {

  import indexBuilder._

  val logger = LoggerFactory.getLogger(this.getClass)

  implicit val system = ActorSystem.create()

  val consumerSettings = ConsumerSettings[String, Array[Byte]](system, new StringDeserializer, new ByteArrayDeserializer)
    .withBootstrapServers("localhost:9092")
    .withGroupId(s"meta-task-worker")
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    .withClientId(s"meta-task-worker")
    //.withPollInterval(java.time.Duration.ofMillis(10L))
   // .withStopTimeout(java.time.Duration.ofHours(1))
  //.withProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1")
  //.withStopTimeout(java.time.Duration.ofSeconds(1000L))

  val committerSettings = CommitterSettings(system).withDelivery(CommitDelivery.waitForAck)

  val producerSettings = ProducerSettings[String, Bytes](system, new StringSerializer, new ByteArraySerializer)
    .withBootstrapServers("localhost:9092")

  val kafkaProducer = producerSettings.createKafkaProducer()
  val settingsWithProducer = producerSettings.withProducer(kafkaProducer)

  def sendResponse(response: MetaTaskResponse): Future[Boolean] = {
    println(s"sending response to topic ${response.responseTopic}...")

    val records = Seq(
      new ProducerRecord[String, Bytes](response.responseTopic,
        response.id, Any.pack(response).toByteArray)
    )

    Source(records)
      .runWith(Producer.plainSink(settingsWithProducer)).map(_ => true)
  }

  def handler(msg: ConsumerMessage.CommittableMessage[String, Array[Byte]]): Future[Boolean] = {
    val task = Any.parseFrom(msg.record.value()).unpack(MetaTask)
    val cmdTask = metaTaskSerializer.deserialize(msg.record.value())

    println(s"${Console.MAGENTA_B} PROCESSING META TASK ${task.id}: ${task} ${Console.RESET}")

    storage.loadIndex(cmdTask.metaId).map(_.get).flatMap { ctx =>
      val meta = new QueryableIndex[K, KeyIndexContext](ctx)(indexBuilder)

      val beforeCommands = Await.result(TestHelper.all(meta.inOrder()), Duration.Inf).map{ x => indexBuilder.ks(x._1) -> x._2.lastChangeVersion}
      println(s"${Console.YELLOW_B}meta before : ${beforeCommands}...${Console.RESET}")

      meta.execute(cmdTask.commands, TestConfig.TX_VERSION).flatMap { result =>
        if(result.error.isDefined) {
          println(result.error.get)
          throw result.error.get
        }

        val afterCommands = Await.result(TestHelper.all(meta.inOrder()), Duration.Inf).map{ x => indexBuilder.ks(x._1) -> (x._2.rangeId, x._2.lastChangeVersion)}
        println(s"${Console.GREEN_B}meta after: ${afterCommands}...${Console.RESET}")

        meta.save().flatMap(ctx => sendResponse(MetaTaskResponse(
          task.id,
          task.metaId,
          task.responseTopic,
          true,
          Some(ctx)
        )))
      }
    }
  }

  val control = Consumer
    .committableSource(consumerSettings, Subscriptions.topics(TestConfig.META_INDEX_TOPIC))
    .mapAsync(1) { msg =>
      handler(msg).map(_ => msg.committableOffset)
    }
    .via(Committer.flow(committerSettings.withMaxBatch(1)))
    .runWith(Sink.ignore)
    .recover {
      case e: RuntimeException => e.printStackTrace()
    }

}