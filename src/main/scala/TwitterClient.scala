import com.twitter.hbc.httpclient.auth._
import com.twitter.hbc.core.{Client, Constants}
import com.twitter.hbc.core.endpoint.{StatusesFilterEndpoint, Location}
import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue, TimeUnit}
import com.twitter.hbc.ClientBuilder
import com.twitter.hbc.core.processor.StringDelimitedProcessor
import ProducerKafka._
import scala.collection.JavaConverters._
import org.apache.log4j.{LogManager, Logger}
// I will use hoosebird client for connecting to twitter

class TwitterClient {

  private val trace_hbc = LogManager.getLogger("console")

  def getClientTwitter(token:String, consumerSecret:String, consumerKey:String, tokenSecret:String, townList:Location):Unit =
  {
    val queue : BlockingQueue[String] = new LinkedBlockingQueue[String](1000000) // Setup a queue
    val auth : Authentication = new OAuth1(consumerKey,consumerSecret,token,tokenSecret)

    val endp : StatusesFilterEndpoint = new StatusesFilterEndpoint()
    endp.locations(List(townList).asJava)

    val clienHbc = new ClientBuilder()
    .hosts(Constants.STREAM_HOST)
    .authentication(auth)
    .gzipEnabled(true)
    .endpoint(endp)
    .processor(new StringDelimitedProcessor(queue))

    val clientHbcComplete =  clienHbc.build()
    clientHbcComplete.connect()

    try
      {
        while (!clientHbcComplete.isDone)
        {
          val tweet = queue.poll(300,TimeUnit.SECONDS)
          getProducerKafka(tweet)
        }
      }
    catch
      {
        case ex: Exception => trace_hbc.error(s" Error in the message. Details: ${ex.printStackTrace()}")
      }
    finally
      {
        clientHbcComplete.stop()
      }
  }

}
