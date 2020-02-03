package retail

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.amazonaws.services.dynamodbv2.document.{DynamoDB, Item}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import retail.GetAverageTimeForFlyer.userdata
import retail.SparkKinesisIntegration.{conf, table}

import scala.util.parsing.json.JSONObject
import org.apache.spark.sql._

object BatchProcessJob {

  val sc = new SparkContext(conf)
  val spark = SparkSession.builder().appName("Batch Job").getOrCreate()

  spark.sql("use <db schema name>")

  // Assuming Hive table name is userdata

  val userdata = spark.sql("select start_timestamp,user_id,event_type,flyer_id,merchant_id,end_timestamp from userdata")

  val flyerOpenUsers = userdata.filter(col("event_type") === "flyer_open")
  // Calculating time spent on each flyer in seconds, as we are interested only in event_type = 'flyer_open'
  val flyerOpenUsersWithduration = flyerOpenUsers.withColumn("time_spent",(col("end_timestamp").cast("long") - col("start_timestamp").cast("long")))
  // Grouping by user_id, flyer_id and merchant_id and calculating count and average to record metrics
  val averageTimeSpentPerFlyer = flyerOpenUsersWithduration.groupBy("user_id","flyer_id","merchant_id")
                                  .agg(round(avg("time_spent"),2).alias("average_time"),count(lit(1))
                                  .alias("count"))


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
}


