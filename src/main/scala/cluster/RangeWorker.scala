package cluster

import akka.actor.ActorSystem
import akka.kafka.scaladsl.{Committer, Consumer, Producer}
import akka.kafka.{CommitDelivery, CommitterSettings, ConsumerMessage, ConsumerSettings, ProducerSettings, Subscriptions}
import akka.stream.scaladsl.{Sink, Source}
import cluster.ClusterCommands.MetaCommand
import cluster.grpc.{KeyIndexContext, MetaTask, MetaTaskResponse, RangeTask}
import cluster.helpers.{TestConfig, TestHelper}
import com.google.protobuf.ByteString
import com.google.protobuf.any.Any
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer, StringDeserializer, StringSerializer}
import org.slf4j.LoggerFactory
import services.scalable.index.{Bytes, Commands, IndexBuilder}

import java.util.UUID
import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, Promise}

class RangeWorker[K, V](val id: String, intid: Int)(implicit val rangeBuilder: RangeBuilder[K, V],
                                                    val clusterIndexBuilder: IndexBuilder[K, KeyIndexContext]) {

  import clusterIndexBuilder._
  implicit val session = rangeBuilder.session

  val RESPONSE_TOPIC = s"${TestConfig.RESPONSE_TOPIC}-$intid"

  val logger = LoggerFactory.getLogger(this.getClass)

  implicit val system = ActorSystem.create()

  val taskConsumerSettings = ConsumerSettings[String, Array[Byte]](system, new StringDeserializer, new ByteArrayDeserializer)
    .withBootstrapServers("localhost:9092")
    .withGroupId(s"range-task-worker-${intid}")
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    .withClientId(s"range-task-worker-${intid}")
    //.withPollInterval(java.time.Duration.ofMillis(10L))
   // .withStopTimeout(java.time.Duration.ofHours(1))
  //.withProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1")
  //.withStopTimeout(java.time.Duration.ofSeconds(1000L))

  val committerSettings = CommitterSettings(system).withDelivery(CommitDelivery.waitForAck)

  val producerSettings = ProducerSettings[String, Bytes](system, new StringSerializer, new ByteArraySerializer)
    .withBootstrapServers("localhost:9092")

  val kafkaProducer = producerSettings.createKafkaProducer()
  val settingsWithProducer = producerSettings.withProducer(kafkaProducer)

  val metaTasks = TrieMap.empty[String, Promise[Boolean]]

  def sendMetaTask(task: MetaCommand[K]): Future[Boolean] = {
    val pr = Promise[Boolean]()

    val buf = rangeBuilder.metaCommandSerializer.serialize(task)
    val records = Seq(
      new ProducerRecord[String, Bytes](TestConfig.META_INDEX_TOPIC, task.id, buf)
    )

   // println(s"sending meta task to topic ${rangeBuilder.metaCommandSerializer.deserialize(buf).responseTopic}...")

    metaTasks.put(task.id, pr)

    Source(records)
      .runWith(Producer.plainSink(settingsWithProducer)).flatMap(_ => pr.future)
  }

  def handler(msg: ConsumerMessage.CommittableMessage[String, Array[Byte]]): Future[Boolean] = {
    val task = Any.parseFrom(msg.record.value()).unpack(RangeTask)
    val cmdTask = rangeBuilder.rangeCommandSerializer.deserialize(msg.record.value())

    println(s"${Console.GREEN_B}processing task ${task.id}...${Console.RESET}")

    val version = UUID.randomUUID().toString
    val commands = cmdTask.commands

    def checkAfterExecution(cindex: ClusterIndex[K, V], previousMax:(K, Option[String])): Future[Boolean] = {

      val range = cindex.ranges(task.rangeId)

      def removeFromMeta(): Future[Boolean] = {
        val metaTask = MetaCommand[K](
          UUID.randomUUID.toString,
          TestConfig.CLUSTER_INDEX_NAME,
          Seq(Commands.Remove(
            TestConfig.CLUSTER_INDEX_NAME,
            Seq(previousMax)
          )),
          RESPONSE_TOPIC
        )

        println(s"${Console.RED_B}SENDING REMOVING TASK ${task.id} TO META FOR RANGE ID ${task.rangeId}... ${Console.RESET}")

        sendMetaTask(metaTask)
      }

      def updateOrRemove(): Future[Boolean] = {
        val createdRanges = cindex.ranges.size > 1
        val currentMax = range.max._1
        val maxChanged = !rangeBuilder.ordering.equiv(previousMax._1, currentMax)

        if (createdRanges || maxChanged) {

          val metaAfter = Await.result(TestHelper.all(cindex.meta.inOrder()), Duration.Inf)

          val insertRanges: Seq[Commands.Command[K, KeyIndexContext]] = metaAfter.map { case (k, ctx, vs) =>
            Commands.Insert(ctx.rangeId, Seq(Tuple3(k, ctx, false)), Some(version))
          }

          val removeRanges: Seq[Commands.Command[K, KeyIndexContext]] = if(maxChanged) Seq(
            Commands.Remove[K, KeyIndexContext](TestConfig.CLUSTER_INDEX_NAME, Seq(previousMax), Some(version))
          ) else Seq.empty[Commands.Command[K, KeyIndexContext]]

          val metaTask = MetaCommand[K](
            UUID.randomUUID.toString,
            TestConfig.CLUSTER_INDEX_NAME,
            removeRanges ++ insertRanges,
            RESPONSE_TOPIC
          )

          println(s"${Console.MAGENTA_B}SENDING UPDATE/INSERT META TASK ${task.id}${Console.RESET}")

          return sendMetaTask(metaTask)
        }

        println(s"${Console.GREEN_B}NORMAL OPERATIONS FOR ${task.id}...${Console.RESET}")

        Future.successful(true)
      }

      if(range.isEmpty()){
        return removeFromMeta()
      }

      updateOrRemove()
    }

    def execute(cindex: ClusterIndex[K, V]): Future[Boolean] = {
        val previousMax = cindex.ranges(task.rangeId).max

        cindex.execute(commands, version).map { br =>
          if(br.error.isDefined) {
            println(br.error.get)
            throw br.error.get
          }
          br.success
        }
        .flatMap(_ => cindex.saveIndexes())
        .flatMap(_ => checkAfterExecution(cindex, (previousMax._1, Some(previousMax._3))))
    }

    ClusterIndex.fromRangeIndexId[K, V](task.rangeId, TestConfig.MAX_RANGE_ITEMS).flatMap(execute)
  }

    Consumer
    .committableSource(taskConsumerSettings, Subscriptions.topics(s"${TestConfig.RANGE_INDEX_TOPIC}-${intid}"))
    .mapAsync(1) { msg =>
      handler(msg).map(_ => msg.committableOffset)
    }
    .via(Committer.flow(committerSettings.withMaxBatch(1)))
    .runWith(Sink.ignore)
    .recover {
      case e: RuntimeException => e.printStackTrace()
    }

  val responseConsumerSettings = ConsumerSettings[String, Array[Byte]](system, new StringDeserializer, new ByteArrayDeserializer)
    .withBootstrapServers("localhost:9092")
    .withGroupId(s"range-task-responses-${intid}")
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    .withClientId(s"range-task-responses-${intid}")

  def responseHandler(msg: ConsumerMessage.CommittableMessage[String, Array[Byte]]): Future[Boolean] = {
    val status = Any.parseFrom(msg.record.value()).unpack(MetaTaskResponse)

    metaTasks.remove(status.id).map(_.success(true))

    Future.successful(true)
  }

  Consumer
    .committableSource(responseConsumerSettings, Subscriptions.topics(s"${TestConfig.RESPONSE_TOPIC}-${intid}"))
    .mapAsync(1) { msg =>
      responseHandler(msg).map(_ => msg.committableOffset)
    }
    .via(Committer.flow(committerSettings.withMaxBatch(1)))
    .runWith(Sink.ignore)
    .recover {
      case e: RuntimeException => e.printStackTrace()
    }
}
