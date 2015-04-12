import java.util.Properties
import java.util.concurrent.LinkedBlockingQueue

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}

class KafkaProducer(val messages: LinkedBlockingQueue[(String, String)]) extends Runnable {
  def run() {
    val props = new Properties()
    props.put("metadata.broker.list", "localhost:9092")
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("request.required.acks", "1")
    val config = new ProducerConfig(props)
    val producer = new Producer[String, String](config)
    var process = 1

    while (process == 1) {
      val message = messages.take
      println(message)
      val data = new KeyedMessage[String, String]("test", message._1, message._2)
      message match {
        case ("STOP", "STOP") => process = 0
        case _ => producer.send(data)
      }
    }
    producer.close()
  }
}