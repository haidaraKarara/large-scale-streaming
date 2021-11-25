import org.apache.kafka.clients.producer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.log4j.{LogManager, Logger}

import java.util._
import org.apache.kafka.common.security.auth.SecurityProtocol

object ProducerKafka {
  private val trace_kafka = LogManager.getLogger("console")

  def getProducerKafka(message: String): Unit =
    {
      val props = new Properties()
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"")
      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,".apache.kafka.common.serialization.StringSerializer")
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,".apache.kafka.common.serialization.StringSerializer")
      props.put(ProducerConfig.ACKS_CONFIG,"all")
      props.put("security.protocol","SASL_PLAINTEXT")

      val topic: String = ""

      try{
        val twitterProducer = new KafkaProducer[String,String](props) // to connect to the cluster
        val twitterRecord = new ProducerRecord[String,String](topic,message)
        twitterProducer.send(twitterRecord)
      }
      catch {
        case ex: Exception => trace_kafka.error(s"Error when sending the message. Details: ${ex.printStackTrace()}")
      }
    }
}
