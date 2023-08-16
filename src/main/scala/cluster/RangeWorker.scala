package cluster

import akka.actor.ActorSystem
import akka.grpc.GrpcClientSettings
import akka.kafka.scaladsl.{Committer, Consumer, Producer}
import akka.kafka._
import akka.stream.scaladsl.{Sink, Source}
import cluster.ClusterCommands.{MetaCommand, RangeCommand}
import cluster.grpc.{ClusterClientResponseServiceClient, KeyIndexContext, MetaTaskResponse, RangeTask, RangeTaskResponse}
import cluster.helpers.{TestConfig, TestHelper}
import com.google.protobuf.any.Any
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer, StringDeserializer, StringSerializer}
import org.slf4j.LoggerFactory
import services.scalable.index.grpc.IndexContext
import services.scalable.index.{Bytes, Commands, IndexBuilder}

import java.util.UUID
import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, Promise}
import scala.util.hashing.MurmurHash3

class RangeWorker[K, V](val id: String, intid: Int)(implicit val rangeBuilder: RangeBuilder[K, V],
                                                    val clusterIndexBuilder: IndexBuilder[K, KeyIndexContext]) {

  import clusterIndexBuilder._
  implicit val session = rangeBuilder.session

  val RESPONSE_TOPIC = s"${TestConfig.RESPONSE_TOPIC}-$intid"

  val logger = LoggerFactory.getLogger(this.getClass)

  val conf =
    ConfigFactory.parseString("akka.http.server.enable-http2 = on").withFallback(ConfigFactory.defaultApplication())

  implicit val system = ActorSystem.create(s"range-worker-$intid", conf)

  val taskConsumerSettings = ConsumerSettings[String, Array[Byte]](system, new StringDeserializer, new ByteArrayDeserializer)
    .withBootstrapServers("localhost:9092")
    .withGroupId(s"range-task-worker-${intid}")
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    .withClientId(s"range-task-worker-${intid}")
    //.withPollInterval(java.time.Duration.ofMillis(10L))
   // .withStopTimeout(java.time.Duration.ofHours(1))
    .withProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1")
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

  def sendResponse(response: RangeTaskResponse): Future[Boolean] = {
    println(s"sending response of task ${response.id}...")

    val records = Seq(
      new ProducerRecord[String, Bytes](response.responseTopic,
        response.id, Any.pack(response).toByteArray)
    )

    Source(records)
      .runWith(Producer.plainSink(settingsWithProducer)).map(_ => true)
  }

  def process(msg: Array[Byte]): Future[Boolean] = {
    val task = Any.parseFrom(msg).unpack(RangeTask)
    val cmdTask = rangeBuilder.rangeCommandSerializer.deserialize(msg)

    println(s"${Console.GREEN_B}processing task ${task.id}...${Console.RESET}")

    val version = TestConfig.TX_VERSION //UUID.randomUUID().toString
    val commandList = cmdTask.commands

    val updates = commandList.filter(_.isInstanceOf[Commands.Update[K, V]])
    val insertions = commandList.filter(_.isInstanceOf[Commands.Insert[K, V]])
    val removals = commandList.filter(_.isInstanceOf[Commands.Remove[K, V]])

    val commands = updates ++ insertions ++ removals

    if (!(commands.forall(_.version.get == TestConfig.TX_VERSION))) {
      println()
    }

    def checkAfterExecution(cindex: ClusterIndex[K, V], previousMax: (K, Option[String])): Future[Boolean] = {

      val range = cindex.ranges(task.rangeId)

      def removeFromMeta(): Future[Boolean] = {
        val metaTask = MetaCommand[K](
          UUID.randomUUID.toString,
          task.indexId,
          Seq(Commands.Remove(
            task.indexId,
            Seq(previousMax)
          )),
          RESPONSE_TOPIC
        )

        println(s"${Console.RED_B}SENDING REMOVING TASK ${task.id} TO META FOR RANGE ID ${task.rangeId} REMOVE KEYS: ${previousMax}... ${Console.RESET}")

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

          val removeRanges: Seq[Commands.Command[K, KeyIndexContext]] = if (maxChanged) Seq(
            Commands.Remove[K, KeyIndexContext](TestConfig.CLUSTER_INDEX_NAME, Seq(previousMax), Some(version))
          ) else Seq.empty[Commands.Command[K, KeyIndexContext]]

          val metaTask = MetaCommand[K](
            UUID.randomUUID.toString,
            task.indexId,
            removeRanges ++ insertRanges,
            RESPONSE_TOPIC
          )

          println(s"${Console.MAGENTA_B}SENDING UPDATE/INSERT META TASK ${task.id} WITH REMOVE SET: ${previousMax} AND INSERT SET: ${metaAfter.map(_._1)}${Console.RESET}")

          return sendMetaTask(metaTask)
        }

        println(s"${Console.GREEN_B}NORMAL OPERATIONS FOR ${task.id}...${Console.RESET}")

        Future.successful(true)
      }

      if (range.isEmpty()) {
        return removeFromMeta()
      }

      updateOrRemove()
    }

    def execute(cindex: ClusterIndex[K, V]): Future[Boolean] = {
      val previousMax = rangeBuilder.ks.deserialize(task.keyInMeta.key.toByteArray)

      cindex.execute(commands, version).map { br =>

        if (br.error.isDefined) {
          println(br.error.get)
          throw br.error.get
        }

        assert(br.success)

        br.success
      }
        .flatMap(_ => cindex.saveIndexes())
        .flatMap(_ => checkAfterExecution(cindex, (previousMax, Some(task.keyInMeta.version))))
    }

    // Configure the client by code:
    val clientSettings = GrpcClientSettings.connectToServiceAt("127.0.0.1", task.responseTopic.toInt)
      .withTls(false)

    // Or via application.conf:
    // val clientSettings = GrpcClientSettings.fromConfig(GreeterService.name)

    // Create a client-side stub for the service
    val client: ClusterClientResponseServiceClient = ClusterClientResponseServiceClient(clientSettings)

    TestHelper.getRange(task.rangeId).map(_.get).flatMap { rangeMeta =>
      val rangeVersion = rangeMeta.lastChangeVersion
      val hasChanged = task.lastChangeVersion.compareTo(rangeVersion) != 0

      assert(!rangeMeta.data.isEmpty || hasChanged)

      println(s"${Console.YELLOW_B}CHECKING VERSION FOR ${task.rangeId}... last version: ${task.lastChangeVersion} meta version: ${rangeMeta.lastChangeVersion} IS EMPTY: ${rangeMeta.data.isEmpty} ${Console.RESET}")

      if (hasChanged) {
        println(s"\n\n${Console.RED_B} RANGE ${task.rangeId} HAS CHANGED FROM ${task.lastChangeVersion} to ${rangeVersion}... REDIRECTING OPERATIONS...${Console.RESET}\n\n")

        // Retry on client side...
        //sendResponse(RangeTaskResponse(task.id, task.responseTopic, false))

        client.respond(RangeTaskResponse(task.id, task.responseTopic, false)).map(_.ok)
      } else {
        ClusterIndex.fromRangeIndexId[K, V](task.rangeId, TestConfig.MAX_RANGE_ITEMS)
          .flatMap(execute)
          .flatMap { res =>
            //sendResponse(RangeTaskResponse(task.id, task.responseTopic, true))
            client.respond(RangeTaskResponse(task.id, task.responseTopic, true)).map(_.ok)
          }
      }
    }.flatMap(_ => client.close().map(_ => true))
  }

  def handler(msg: ConsumerMessage.CommittableMessage[String, Array[Byte]]): Future[Boolean] = {
    process(msg.record.value())
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
    /*.recover {
      case e: RuntimeException => e.printStackTrace()
    }*/
}
