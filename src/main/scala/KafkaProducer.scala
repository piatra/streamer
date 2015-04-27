import java.util.Properties

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}

class KafkaProducer {
  def putTweet(user: String, value: String): Unit = {
    val props = new Properties()
    props.put("metadata.broker.list", "localhost:9092")
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("request.required.acks", "1")
    val config = new ProducerConfig(props)
    val producer: Producer[String, String] = new Producer[String, String](config)
    val data = new KeyedMessage[String, String]("javascript", user, value)
    producer.send(data)
  }
}