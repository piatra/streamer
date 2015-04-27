import java.util.Properties

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}

class KafkaProducer {

  val props = new Properties()
  props.put("metadata.broker.list", "localhost:9092")
  props.put("serializer.class", "kafka.serializer.StringEncoder")
  props.put("request.required.acks", "1")
  val config = new ProducerConfig(props)
  var producer: Producer[String, String] = new Producer[String, String](config)

  def putTweet(user: String, value: String): Unit = {
    val data = new KeyedMessage[String, String]("test", user, value)
    this.producer.send(data)
  }

}