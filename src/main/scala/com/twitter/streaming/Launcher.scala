package com.twitter.streaming

import java.util.Properties

import com.twitter.hbc.core.endpoint.Location
import org.apache.log4j.{LogManager, Logger}

import scala.io.Source

object Launcher extends App {

  private var logger: Logger = LogManager.getLogger("Log_Console")

  val url = getClass.getResource("streaming.properties")
  val properties: Properties = new Properties()
  val source = Source.fromURL(url)
  properties.load(source.bufferedReader())

  val consumer_key = properties.getProperty("CONSUMER_KEY")
  val consumer_secret = properties.getProperty("CONSUMER_SECRET")
  val access_token = properties.getProperty("ACCESS_TOKEN")
  val token_secret = properties.getProperty("TOKEN_SECRET")
  val kafkaServers = properties.getProperty("kafkaBootStrapServers")
  val topic = properties.getProperty("topic")

  val groupId = properties.getProperty("consumerGroupId")
  val readOrder = properties.getProperty("consumerReadOrder")
  val sparkBacthDuration = properties.getProperty("batchDuration")
  val mySQLHost_ = properties.getProperty("mySQLHost")
  val mySQLUser_ = properties.getProperty("mySQLUser")
  val mySQLPwd_ = properties.getProperty("mySQLPwd")
  val mySQLDatabase_ = properties.getProperty("mySQLDatabase")

  // coordinates of the canada. You can get the coordinate of a place here : https://boundingbox.klokantech.com/
  val country = new Location(new Location.Coordinate(-141.0, 41.7),
    new Location.Coordinate(-52.3, 83.3))

  val twitterClient: Unit = TwitterClient.getClientTwitter(access_token, consumer_secret, consumer_key, token_secret, country, kafkaServers, topic)


}
