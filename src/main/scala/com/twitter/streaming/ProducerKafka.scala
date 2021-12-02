package com.twitter.streaming

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.log4j.LogManager

import java.util.Properties

object ProducerKafka {
  private val trace_kafka = LogManager.getLogger("Log_Console")

  def getProducerKafka(message: String, kafkaServer: String, topic: String): Unit = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ".apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ".apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.ACKS_CONFIG, "all")
    props.put("security.protocol", "SASL_PLAINTEXT")

    try {
      val twitterProducer = new KafkaProducer[String, String](props) // to connect to the cluster
      val twitterRecord = new ProducerRecord[String, String](topic, message)
      twitterProducer.send(twitterRecord)
    }
    catch {
      case ex: Exception => trace_kafka.error(s"Error when sending the message. Details: ${ex.printStackTrace()}")
    }
  }
}
