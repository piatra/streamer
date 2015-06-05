import java.util
import java.util.{Random, Properties}
import java.util.concurrent.{ExecutorService, Executors, LinkedBlockingQueue}

import kafka.consumer.{ConsumerTimeoutException, ConsumerConfig, KafkaStream}
import kafka.javaapi.consumer.ConsumerConnector
import kafka.utils.ZkUtils

import scala.collection.JavaConversions._

class QueueConsumer(a_stream: KafkaStream[Array[Byte], Array[Byte]], a_threadNumber: Int,
                   queue: LinkedBlockingQueue[(String, String)]) extends Runnable {

  var m_stream: KafkaStream[Array[Byte], Array[Byte]] = a_stream
  var m_threadNumber: Integer = a_threadNumber

  def run() {
    val it = m_stream.iterator()
    while (it.hasNext()) {
      val next = it.next()
      queue.put((new String(next.key()), new String(next.message())))
    }
  }
}

class SyncQueueConsumer(a_stream: KafkaStream[Array[Byte], Array[Byte]], queue: LinkedBlockingQueue[(String, String)]) {

  def getAll() {
    val it = a_stream.iterator()
    var hasNext = true
    try {
      while (hasNext) {
        val next = it.next()
        queue.put((new String(next.key()), new String(next.message())))
      }
    } catch {
      case e: ConsumerTimeoutException => hasNext = false
      case _ => println("Some other sync error but probably out of messages")
    }
  }

}

class KafkaConsumer(a_topic: String, queue: LinkedBlockingQueue[(String, String)]) {

  val rand = new Random()
  val zooKeeper: String = "localhost:2181"
  val groupId: String = rand.nextInt(40).toString

  var consumer: ConsumerConnector = kafka.consumer.Consumer
                                              .createJavaConsumerConnector(createConsumerConfig(zooKeeper, groupId))
  var topic: String = a_topic
  var executor: ExecutorService = _

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
      executor.submit(new QueueConsumer(stream, threadNumber, queue))
      threadNumber += 1
    }
  }

  def createConsumerConfig(a_zookeeper: String, a_groupId: String): ConsumerConfig = {
    val props: Properties = new Properties()

    // Do not reset read from beginning
//    ZkUtils.maybeDeletePath(a_zookeeper, "/consumers/" + new String(a_groupId))

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

class SyncKafkaConsumer(a_topic: String, queue: LinkedBlockingQueue[(String, String)]) {

  val zooKeeper: String = "localhost:2181"
  val groupId: String = "1"

  var consumer: ConsumerConnector = kafka.consumer.Consumer
    .createJavaConsumerConnector(createConsumerConfig(zooKeeper, groupId))
  var topic: String = a_topic

  def getAll() {
    val topicCountMap: util.Map[String, Integer] = new util.HashMap[String, Integer]()
    topicCountMap.put(topic, new Integer(1))
    val consumerMap: util.Map[String, util.List[KafkaStream[Array[Byte], Array[Byte]]]] = consumer.createMessageStreams(topicCountMap)
    val streams: util.List[KafkaStream[Array[Byte], Array[Byte]]] = consumerMap.get(topic)

    // now create an object to consume the messages
    for (stream <- streams.iterator()) {
      (new SyncQueueConsumer(stream, queue)).getAll()
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
    props.put("consumer.timeout.ms", "2000")
    props.put("auto.offset.reset", "smallest")

    new ConsumerConfig(props)
  }
}