package cluster

import akka.actor.ActorSystem
import akka.grpc.GrpcClientSettings
import akka.kafka.ConsumerMessage.CommittableOffsetBatch
import akka.kafka.scaladsl.{Committer, Consumer, Producer}
import akka.kafka._
import akka.stream.{ActorAttributes, Supervision}
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
import services.scalable.index.{Bytes, Commands, IndexBuilder, QueryableIndex}

import java.util.UUID
import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.{Duration, DurationInt}
import scala.concurrent.{Await, Future, Promise}
import scala.util.control.NonFatal
import scala.util.hashing.MurmurHash3

class RangeWorker[K, V](val id: String, intid: Int)(implicit val rangeBuilder: IndexBuilder[K, V],
                                                    val clusterIndexBuilder: IndexBuilder[K, KeyIndexContext],
                                                    val rangeCommandSerializer: GrpcRangeCommandSerializer[K, V],
                                                    val metaCommandSerializer: GrpcMetaCommandSerializer[K]) {

  import clusterIndexBuilder._
  //implicit val session = rangeBuilder.session

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
    .withProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "50")
  //.withStopTimeout(java.time.Duration.ofSeconds(1000L))

  val committerSettings = CommitterSettings(system).withDelivery(CommitDelivery.waitForAck)

  val producerSettings = ProducerSettings[String, Bytes](system, new StringSerializer, new ByteArraySerializer)
    .withBootstrapServers("localhost:9092")

  val kafkaProducer = producerSettings.createKafkaProducer()
  val settingsWithProducer = producerSettings.withProducer(kafkaProducer)

  val metaTasks = TrieMap.empty[String, Promise[Boolean]]

  def sendMetaTask(task: MetaCommand[K]): Future[Boolean] = {
    val pr = Promise[Boolean]()

    val buf = metaCommandSerializer.serialize(task)
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

  def process(changed: Seq[(grpc.RangeTask, ConsumerMessage.CommittableMessage[String, Array[Byte]])],
              notChanged: Seq[(grpc.RangeTask, ConsumerMessage.CommittableMessage[String, Array[Byte]])]): Future[Boolean] = {

    if(notChanged.isEmpty){
      return Future.sequence(changed.map { case (r, _) =>
        // Configure the client by code:
        val clientSettings = GrpcClientSettings.connectToServiceAt("127.0.0.1", r.responseTopic.toInt)
          .withTls(false)

        // Create a client-side stub for the service
        val client: ClusterClientResponseServiceClient = ClusterClientResponseServiceClient(clientSettings)

        client.respond(RangeTaskResponse(r.id, r.responseTopic, false, true))
          .map(_.ok)
          .flatMap(ok => client.close().map(_ => ok))
      }).map(_ => true)
    }

    val fusedCmds = notChanged.map { case (r, _) => r.commands }.flatten
    val task = notChanged.head._1//.withCommands(fusedCmds)

    //val msg = Any.pack(rc).toByteArray

    //val task = Any.parseFrom(msg).unpack(RangeTask)
    //val cmdTask = rangeCommandSerializer.deserialize(msg)

    val cmds = fusedCmds.map { c => rangeCommandSerializer.commandsSerializer.deserialize(c.toByteArray) }

    println(s"${Console.GREEN_B}processing task ${task.id}...${Console.RESET}")

    val version = TestConfig.TX_VERSION //UUID.randomUUID().toString
    val commandList = cmds

    val updates = commandList.filter(_.isInstanceOf[Commands.Update[K, V]])
    val insertions = commandList.filter(_.isInstanceOf[Commands.Insert[K, V]])
    val removals = commandList.filter(_.isInstanceOf[Commands.Remove[K, V]])

    val commands = updates ++ insertions ++ removals

    def checkAfterExecution(cindex: ClusterIndex[K, V], previousMax: (K, Option[String])): Future[(Boolean, Boolean)] = {

      val range = cindex.ranges(task.rangeId)

      def removeFromMeta(): Future[(Boolean, Boolean)] = {
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

        sendMetaTask(metaTask).map(_ -> true)
      }

      def updateOrRemove(): Future[(Boolean, Boolean)] = {
        val createdRanges = cindex.ranges.size > 1
        val currentMax = Await.result(range.max.map(_.get), Duration.Inf)
        val maxChanged = !rangeBuilder.ord.equiv(previousMax._1, currentMax)

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

          return sendMetaTask(metaTask).map(_ -> true)
        }

        println(s"${Console.GREEN_B}NORMAL OPERATIONS FOR ${task.id}...${Console.RESET}")

        Future.successful(true -> false)
      }

      if (range.isEmpty()) {
        return removeFromMeta()
      }

      updateOrRemove()
    }

    def execute(cindex: ClusterIndex[K, V]): Future[(Boolean, Boolean)] = {
      val previousMax = rangeBuilder.keySerializer.deserialize(task.keyInMeta.key.toByteArray)

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

    // Or via application.conf:
    // val clientSettings = GrpcClientSettings.fromConfig(GreeterService.name)

    Future.sequence(changed.map { case (r, _) =>
      // Configure the client by code:
      val clientSettings = GrpcClientSettings.connectToServiceAt("127.0.0.1", r.responseTopic.toInt)
        .withTls(false)

      // Create a client-side stub for the service
      val client: ClusterClientResponseServiceClient = ClusterClientResponseServiceClient(clientSettings)

      client.respond(RangeTaskResponse(r.id, r.responseTopic, false, true))
        .map(_.ok)
      .flatMap(ok => client.close().map(_ => ok))
    })
    .flatMap { _ =>
      ClusterIndex.fromRangeIndexId[K, V](task.rangeId, TestConfig.MAX_RANGE_ITEMS)
        .flatMap(execute)
        .flatMap { case (ok, changed) =>
          Future.sequence(notChanged.map { case (r, _) =>
            // Configure the client by code:
            val clientSettings = GrpcClientSettings.connectToServiceAt("127.0.0.1", r.responseTopic.toInt)
              .withTls(false)

            // Create a client-side stub for the service
            val client: ClusterClientResponseServiceClient = ClusterClientResponseServiceClient(clientSettings)

            client.respond(RangeTaskResponse(r.id, r.responseTopic, true, false))
              .map(_.ok)
            .flatMap(ok => client.close().map(_ => ok))

          }).map(_.forall(_ == true))
        }
    }
  }

  /*def handler(msg: ConsumerMessage.CommittableMessage[String, Array[Byte]]): Future[Boolean] = {
    process(msg.record.value())
  }*/

  /*def handler(records: Seq[ConsumerMessage.CommittableMessage[String, Array[Byte]]]): Future[Boolean] = {
    val msgs = TrieMap.empty[String, Seq[(RangeTask, ConsumerMessage.CommittableMessage[String, Array[Byte]])]]

    records.map { m =>
      Any.parseFrom(m.record.value()).unpack(RangeTask) -> m
    }.groupBy(_._1.rangeId).foreach { case (r, list) =>
      msgs.put(r, list)
    }

    def processBatch(): Future[Boolean] = {
      if(msgs.isEmpty) return Future.successful(true)

      var list = Seq.empty[(RangeTask, ConsumerMessage.CommittableMessage[String, Array[Byte]])]

      msgs.foreach { case (range, l) =>
        if(l.isEmpty){
          msgs.remove(range)
        } else {
          val e = l.head
          list = list :+ e

          msgs.update(range, l.filterNot{x => x._1.id == e._1.id})
        }
      }

      Future.sequence(list.map { case (t, m) =>
        process(m.record.value())
      }).map(_.forall(_ == true)).flatMap(_ => processBatch())
    }

    processBatch()
  }*/

  def handler(records: Seq[ConsumerMessage.CommittableMessage[String, Array[Byte]]]): Future[Boolean] = {

    val conversions = records.map { m =>
      Any.parseFrom(m.record.value()).unpack(RangeTask) -> m
    }.groupBy(_._1.rangeId)

    val readVersions = Future.sequence(conversions.map { case (range, list) =>
      storage.loadIndex(range).map(_.get).map { rangeMeta =>
        range -> (rangeMeta, list)
      }
    })

    readVersions.flatMap { versions =>

      val flattened = versions.map { case (range, (ctx, list)) =>
        val changed = list.filterNot{case (t, m) => t.lastChangeVersion == ctx.lastChangeVersion}
        val notChanged = list.filter{case (t, m) => t.lastChangeVersion == ctx.lastChangeVersion}

        range -> Tuple2(changed, notChanged)
      }

      Future.sequence(flattened.map { case (range, (changed, notChanged)) =>
        process(changed, notChanged)
      }).map(_.forall(_ == true))
    }
  }

  val decider: Supervision.Decider = {
    case t =>
      t.printStackTrace()
      Supervision.Stop
  }

  val rangeg = Consumer
  .committableSource(taskConsumerSettings, Subscriptions.topics(s"${TestConfig.RANGE_INDEX_TOPIC}-${intid}"))
  .groupedWithin(50, 100.millis)
  .mapAsync(1) { msgs =>
    handler(msgs).map(_ => msgs.map(_.committableOffset))
  }
  .map { msgs =>
    //val maxOffset = msgs.maxBy(_.partitionOffset.offset)
    //println(s"max offset: ${maxOffset}")
    //maxOffset
    CommittableOffsetBatch(msgs.sortBy(_.partitionOffset.offset))
  }
  /*.mapAsync(1){ msg =>
    process(msg.record.value()).map(_ => msg.committableOffset)
  }*/
  .via(Committer.flow(committerSettings.withMaxBatch(50)))
  //.via(Committer.flow(committerSettings.withMaxBatch(1)))
  //.runWith(Sink.ignore)
  .withAttributes(ActorAttributes.supervisionStrategy(decider))

  rangeg.run()

  val responseConsumerSettings = ConsumerSettings[String, Array[Byte]](system, new StringDeserializer, new ByteArrayDeserializer)
    .withBootstrapServers("localhost:9092")
    .withGroupId(s"range-task-responses-${intid}")
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    .withClientId(s"range-task-responses-${intid}")

  /*def responseHandler(msg: ConsumerMessage.CommittableMessage[String, Array[Byte]]): Future[Boolean] = {
    val status = Any.parseFrom(msg.record.value()).unpack(MetaTaskResponse)

    metaTasks.remove(status.id).map(_.success(true))

    Future.successful(true)
  }*/

  def responseHandler(msgs: Seq[ConsumerMessage.CommittableMessage[String, Array[Byte]]]): Future[Boolean] = {
    val statuses = msgs.map { msg =>
      Any.parseFrom(msg.record.value()).unpack(MetaTaskResponse)
    }

    statuses.foreach { s =>
      metaTasks.remove(s.id).map(_.success(true))
    }

    Future.successful(true)
  }

  val responsesg = Consumer
    .committableSource(responseConsumerSettings, Subscriptions.topics(s"${TestConfig.RESPONSE_TOPIC}-${intid}"))
    .groupedWithin(50, 100.millis)
    .mapAsync(1) { msgs =>
      responseHandler(msgs).map(_ => msgs.map(_.committableOffset))
    }
    .map { msgs =>
      //val maxOffset = msgs.maxBy(_.partitionOffset.offset)
      //println(s"max offset: ${maxOffset}")
      //maxOffset
      CommittableOffsetBatch(msgs.sortBy(_.partitionOffset.offset))
    }
    .via(Committer.flow(committerSettings.withMaxBatch(50)))
    .withAttributes(ActorAttributes.supervisionStrategy(decider))

  responsesg.run()

}
