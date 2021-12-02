package com.twitter.streaming

import com.twitter.hbc.ClientBuilder
import com.twitter.hbc.core.Constants
import com.twitter.hbc.core.endpoint.{Location, StatusesFilterEndpoint}
import com.twitter.hbc.core.processor.StringDelimitedProcessor
import com.twitter.hbc.httpclient.auth._
import org.apache.log4j.LogManager

import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue, TimeUnit}
import scala.collection.JavaConverters._
// I will use hoosebird client for connecting to twitter

object TwitterClient {

  private val trace_hbc = LogManager.getLogger("console")

  def getClientTwitter(token: String, consumerSecret: String,
                       consumerKey: String, tokenSecret: String, townList: Location,kafkaServers: String, topic: String): Unit = {
    val queue: BlockingQueue[String] = new LinkedBlockingQueue[String](1000000) // Setup a queue
    val auth: Authentication = new OAuth1(consumerKey, consumerSecret, token, tokenSecret)

    val endPoint: StatusesFilterEndpoint = new StatusesFilterEndpoint()
    endPoint.locations(List(townList).asJava)

    val clientHbc = new ClientBuilder()
      .hosts(Constants.STREAM_HOST)
      .authentication(auth)
      .gzipEnabled(true)
      .endpoint(endPoint)
      .processor(new StringDelimitedProcessor(queue))

    val clientHbcComplete = clientHbc.build()
    clientHbcComplete.connect()

    try {
      while (!clientHbcComplete.isDone) {
        val tweet = queue.poll(Launcher.sparkBacthDuration.toLong, TimeUnit.SECONDS)
        ProducerKafka.getProducerKafka(tweet, kafkaServers, topic)
      }
    }
    catch {
      case ex: Exception => trace_hbc.error(s" Error in the message. Details: ${ex.printStackTrace()}")
    }
    finally {
      clientHbcComplete.stop()
    }
  }

}
