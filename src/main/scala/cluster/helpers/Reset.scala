package cluster.helpers

import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}

import scala.jdk.CollectionConverters.{CollectionHasAsScala, SeqHasAsJava}

object Reset {

  def main(args: Array[String]): Unit = {

    val session = TestHelper.getSession()

    println(session.execute("truncate table blocks;").wasApplied())
    println(session.execute("truncate table indexes;").wasApplied())
    println(session.execute("truncate table ranges;").wasApplied())
    println(session.execute("truncate table test_indexes;").wasApplied())

    session.close()

    val config = new java.util.Properties()
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")

    val admin = AdminClient.create(config)

    var topics = Seq(TestConfig.META_INDEX_TOPIC, TestConfig.CLIENT_TOPIC)

    for (i <- 0 until TestConfig.N_PARTITIONS) {
      topics :+= s"${TestConfig.RANGE_INDEX_TOPIC}-$i"
      topics :+= s"${TestConfig.RESPONSE_TOPIC}-$i"
    }

    val existingTopics = admin.listTopics().names().get().asScala.toSeq
    val deleteTopics = existingTopics.filter{t => topics.exists(_ == t)}

    if(!deleteTopics.isEmpty){
      println(admin.deleteTopics(topics.asJava).all().get())
      Thread.sleep(2000L)
    }

    admin.createTopics(topics.map{topic => new NewTopic(topic, 1.toShort, 1.toShort)}.asJava).all().get()

    Thread.sleep(2000L)

    admin.close()
    session.close()
  }

}
