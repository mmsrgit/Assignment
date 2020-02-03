package retail

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kinesis.KinesisInputDStream
import org.apache.spark.streaming.{Duration, Milliseconds, StreamingContext}
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.kinesis.AmazonKinesisClient
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import com.amazonaws.services.dynamodbv2.document.Item
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import org.apache.spark.sql.functions._


import scala.util.parsing.json.JSONObject

object SparkKinesisIntegration extends App {

  val credentials = new DefaultAWSCredentialsProviderChain().getCredentials()
  val kinesisClient = new AmazonKinesisClient(credentials)
  kinesisClient.setEndpoint(args(1))
  val numShards = kinesisClient.describeStream(args(3)).getStreamDescription().getShards().size
  val numStreams = numShards
  val batchInterval = Milliseconds(args(5).toInt)
 val executionMode = args(0)
  val conf = new SparkConf().setAppName("Sparkstreaming and Kinesis integration").setMaster(executionMode)
  val sc = new SparkContext(conf)
  val spark = SparkSession.builder().appName("Sparkstreaming and Kinesis integration").getOrCreate()
  val ssc = new StreamingContext(conf,batchInterval)

  val kinesisStream = (0 until numStreams).map { i =>
    KinesisInputDStream.builder
      .streamingContext(ssc)
      .endpointUrl(args(1))
      .regionName(args(2))
      .streamName(args(3))
      .checkpointAppName(args(4))
      .checkpointInterval(Duration(args(5).toInt))
      .storageLevel(StorageLevel.MEMORY_AND_DISK_2)
      .build()
  }
  // Union all the streams
  val unionStreams = ssc.union(kinesisStream)

  val lines = unionStreams.flatMap(byteArray => new String(byteArray).split("\n"))
  val dataset = spark.read.json("<json dataset from lines>")

  /**
    *  The dataset that we have computed needs to be processed in following way
    *
    *  1. Write to the HIVE DB to store it in batch view
    *  2. Apply algorithm on this dataset and create real-time view and store it in Amazon DynamoDB
    *
    */

  // step 1
  dataset.write.parquet("<HDFS EXTERNAL TABLE LOCATION>")  // HDFS EXTERNAL TABLE LOCATION

  // step 2 - Applying algorithm on dataset and persist in Amazon DB

  val flyerOpenUsers = dataset.filter(col("event_type") === "flyer_open")
  // Calculating time spent on each flyer in seconds, as we are interested only in event_type = 'flyer_open'
  val flyerOpenUsersWithduration = flyerOpenUsers.withColumn("time_spent",(col("end_timestamp").cast("long") - col("start_timestamp").cast("long")))
  // Grouping by user_id, flyer_id and merchant_id and calculating count and average to record metrics
  val averageTimeSpentPerFlyer = flyerOpenUsersWithduration.groupBy("user_id","flyer_id","merchant_id").agg(round(avg("time_spent"),2).alias("average_time"),count(lit(1)).alias("count"))


  val records = averageTimeSpentPerFlyer.collect.map(_.toSeq).flatten

  val client = AmazonDynamoDBClientBuilder.standard.build
  val dynamoDB: DynamoDB = new DynamoDB(client)
  val table = dynamoDB.getTable("<table_name>")

  averageTimeSpentPerFlyer.foreach(element => {
    val item = Item.fromJSON(convertRowToJSON(element))
    table.putItem(item)
  })

  def convertRowToJSON(row: Row): String = {
    val m = row.getValuesMap(row.schema.fieldNames)
    JSONObject(m).toString()
  }

  ssc.awaitTermination()

}

