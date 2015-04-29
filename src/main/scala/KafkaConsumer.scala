import java.util
import java.util.Properties
import java.util.concurrent.{ExecutorService, Executors}

import kafka.consumer.{ConsumerConfig, KafkaStream}
import kafka.javaapi.consumer.ConsumerConnector
import kafka.utils.ZkUtils

import scala.collection.JavaConversions._

class ConsumerTest(a_stream: KafkaStream[Array[Byte], Array[Byte]], a_threadNumber: Int,
                   TwtParser: TweetParser) extends Runnable {

  var m_stream: KafkaStream[Array[Byte], Array[Byte]] = a_stream
  var m_threadNumber: Integer = a_threadNumber

  def run() {
    val it = m_stream.iterator()
    var steps = 100
    while (it.hasNext() && steps > 0) {
      steps = steps - 1
      val next = it.next()
      TwtParser.queueTweet((new String(next.key()), new String(next.message())))
    }
  }
}

class KafkaConsumer(a_topic: String) {

  val zooKeeper: String = "localhost:2181"
  val groupId: String = "1"

  var consumer: ConsumerConnector = kafka.consumer.Consumer
                                              .createJavaConsumerConnector(createConsumerConfig(zooKeeper, groupId))
  var topic: String = a_topic
  var executor: ExecutorService = _

  val TwtParser = new TweetParser()

  def shutdown() {
    if (consumer != null) consumer.shutdown()
    if (executor != null) executor.shutdown()
  }

  def run(a_numThreads: Int) {
    val topicCountMap: util.Map[String, Integer] = new util.HashMap[String, Integer]()
    topicCountMap.put(topic, new Integer(a_numThreads))
    val consumerMap: util.Map[String, util.List[KafkaStream[Array[Byte], Array[Byte]]]] = consumer.createMessageStreams(topicCountMap)
    val streams: util.List[KafkaStream[Array[Byte], Array[Byte]]] = consumerMap.get(topic)

    // now launch all the threads
    executor = Executors.newFixedThreadPool(a_numThreads)

    // now create an object to consume the messages
    var threadNumber: Int = 0
    for (stream <- streams.iterator()) {
      executor.submit(new ConsumerTest(stream, threadNumber, TwtParser))
      threadNumber += 1
    }
  }

  def createConsumerConfig(a_zookeeper: String, a_groupId: String): ConsumerConfig = {
    val props: Properties = new Properties()

    ZkUtils.maybeDeletePath(a_zookeeper, "/consumers/" + new String(a_groupId))

    props.put("zookeeper.connect", a_zookeeper)
    props.put("group.id", a_groupId)
    props.put("auto.offset.reset", "smallest")
    props.put("zookeeper.session.timeout.ms", "400")
    props.put("zookeeper.sync.time.ms", "200")
    props.put("auto.commit.interval.ms", "1000")
    props.put("auto.offset.reset", "smallest")

    new ConsumerConfig(props)
  }
}