import java.util
import java.util.Properties
import java.util.concurrent.{ExecutorService, Executors}

import kafka.consumer.{ConsumerConfig, KafkaStream}
import kafka.javaapi.consumer.ConsumerConnector

import scala.collection.JavaConversions._

class ConsumerTest(a_stream: KafkaStream[Array[Byte], Array[Byte]], a_threadNumber: Int) extends Runnable {
  var m_stream: KafkaStream[Array[Byte], Array[Byte]] = a_stream
  var m_threadNumber: Integer = a_threadNumber

  def run() {
    println("Running thread $m_threadNumber")
    val it = m_stream.iterator()
    while (it.hasNext()) {
      println("Thread " + m_threadNumber + ": " + new String(it.next().message()))
    }
    println("Shutting down Thread: " + m_threadNumber)
  }
}

class ConsumerGroupExample(a_zookeeper: String, a_groupId: String, a_topic: String) {
  println("topic", a_topic)
  var consumer: ConsumerConnector = kafka.consumer.Consumer.createJavaConsumerConnector(
    createConsumerConfig(a_zookeeper, a_groupId))
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
    //
    executor = Executors.newFixedThreadPool(a_numThreads)

    // now create an object to consume the messages
    //
    var threadNumber: Int = 0
    for (stream <- streams.iterator()) {
      executor.submit(new ConsumerTest(stream, threadNumber))
      threadNumber += 1
    }
  }

  def createConsumerConfig(a_zookeeper: String, a_groupId: String): ConsumerConfig = {
    val props: Properties = new Properties()
    println("connect to ", a_zookeeper)
    props.put("zookeeper.connect", a_zookeeper)
    props.put("group.id", a_groupId)
    props.put("auto.offset.reset", "smallest")
    props.put("zookeeper.session.timeout.ms", "400")
    props.put("zookeeper.sync.time.ms", "200")
    props.put("auto.commit.interval.ms", "1000")

    new ConsumerConfig(props)
  }
}