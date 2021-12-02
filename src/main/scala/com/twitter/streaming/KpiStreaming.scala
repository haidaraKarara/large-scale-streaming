package com.twitter.streaming

import com.twitter.streaming.Launcher._
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, current_timestamp, from_json}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KpiStreaming {

  val kpiSchema: StructType = StructType(
    Array(
      StructField("event_date", DateType, nullable = true),
      StructField("id", StringType, nullable = false),
      StructField("text", StringType, nullable = true),
      StructField("lang", StringType, nullable = true),
      StructField("userid", StringType, nullable = false),
      StructField("name", StringType, nullable = false),
      StructField("screenName", StringType, nullable = true),
      StructField("location", StringType, nullable = true),
      StructField("followersCount", IntegerType, nullable = false),
      StructField("retweetCount", IntegerType, nullable = false),
      StructField("favoriteCount", IntegerType, nullable = false),
      StructField("Zipcode", StringType, nullable = true),
      StructField("ZipCodeType", StringType, nullable = true),
      StructField("City", StringType, nullable = true),
      StructField("State", StringType, nullable = true)
    ))

  // parameters of the Spark consumer client, needed to connect to Kafka
  private val bootStrapServers: String = bootStrapServers
  private val consumerGroupId: String = consumerGroupId
  //private val consumerReadOrder: String = ""
  // private val kerberosName : String = ""
  private val batchDuration = batchDuration
  private val topics: Array[String] = Array(topic)
  private val mySQLHost = mySQLHost_
  private val mySQLUser = mySQLUser_
  private val mySQLPwd = mySQLPwd
  private val mySQLDatabase = mySQLDatabase

  private val logger: Logger = LogManager.getLogger("Log_Console")
  //var ss: SparkSession = null
  var spConf: SparkConf = null

  def main(args: Array[String]): Unit = {
    val kafkaParams = Map(
      "bootstrap.server" -> bootStrapServers,
      "group.id" -> consumerGroupId,
      "enable.auto.commit" -> (false: java.lang.Boolean), // to control the acknowledgment: ack
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "security.protocol" -> SecurityProtocol.SASL_PLAINTEXT
      //"sasl.kerberos.service.name" -> ""
    )
    // The consumer
    val ssc = getSparkStreamingContext(batchDuration, env = true)

    try {
      val kafkaConsumer = KafkaUtils.createDirectStream[String, String](
        ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))

      // Read events
      kafkaConsumer.foreachRDD {
        rddKafka => {
          val recordKafka = rddKafka.map(c => c.value())
          val offsets = rddKafka.asInstanceOf[HasOffsetRanges].offsetRanges
          val _sparkSession = SparkSession
            .builder()
            .config(rddKafka.sparkContext.getConf)
            .enableHiveSupport()
            .getOrCreate()
          import _sparkSession.implicits._
          val dfMessage = recordKafka.toDF("kafkaObject")
          if (dfMessage.count() > 0) {
            // Data Parsing
            val dfParsed = messageParsing(dfMessage)
            // Calculate the KPIs
            val dfIndicators = computeIndicators(dfParsed, _sparkSession).cache() // the cache function: need to verify if we have the a sufficient memory
            // Write the KPIs in MySQL
            dfIndicators.write
              .format("jdbc")
              .options(Map(
                "url"-> s"jdbc:mysql://$mySQLHost?zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC",
                "dbtable" -> mySQLDatabase,
                "user" -> mySQLUser,
                "password" -> mySQLPwd
              ))
              .mode("append")
              .save()
          }
          kafkaConsumer.asInstanceOf[CanCommitOffsets].commitAsync(offsets)
        }
      }
    }
    catch {
      case e: Exception => logger.info(s"Fatal error in Spark Application ${e.printStackTrace()}")
    }
    finally {
      ssc.start()
      ssc.awaitTermination()
    }
  }

  def getSparkStreamingContext(batchDuration: Int, env: Boolean): StreamingContext = {
    logger.info("Initialization of spark context streaming")
    if (env) {
      spConf = new SparkConf()
        .setMaster("local[*]")
        .setAppName("My Dev app streaming")
    } else {
      spConf = new SparkConf().setAppName("My Prod app streaming")
    }

    val ssc: StreamingContext = new StreamingContext(spConf, Seconds(batchDuration))
    ssc
  }

  def messageParsing(dfKafkaEvents: DataFrame): DataFrame = {
    logger.info("Parsing of json object received from Kafka...")
    val dfParsing = dfKafkaEvents.withColumn("kafkaObject", from_json(col("kafkaObject"), kpiSchema))
    dfParsing
  }

  def computeIndicators(eventsParsed: DataFrame, ss: SparkSession): DataFrame = {
    logger.info("Calculating the KPI...")
    eventsParsed.createTempView("messages")
    val dfKpi = ss.sql(
      """
           SELECT t.City,
               count(t.id) OVER (PARTITION BY t.City ORDER BY City) as tweetCount,
               sum(t.bin_retweet) OVER (PARTITION BY t.City ORDER BY City) as retweetCount
           FROM  (
              SELECT date_event, id, City, CASE WHEN retweetCount > 0 THEN 1 ELSE 0 END AS bin_retweet  FROM messages
           ) t
      """
    ).withColumn("date_events", current_timestamp())
      .select(
        col("date_events").alias("the events date"),
        col("city").alias("events city"),
        col("tweetCount").alias("Number of tweets per city"),
        col("retweetCount").alias("Number of RT per city")
      )
    dfKpi
  }
}
