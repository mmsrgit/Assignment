package retail

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._



object GetAverageTimeForFlyer extends App {

  val conf = new SparkConf().setMaster("local").setAppName("GetAverageTimeForFlyer")
  val sc = new SparkContext(conf)
  //val hc = new HiveContext(sc)
  //SparkSession.builder.enableHiveSupport();
  val spark = SparkSession.builder().appName("GetAverageTimeForFlyer").getOrCreate()

  val userdata = spark.read.option("header","true")
                            .schema("start_timestamp TIMESTAMP, user_id STRING, event_type STRING, flyer_id INT, merchant_id INT, end_timestamp TIMESTAMP")
                            .csv("/Users/RAMA/Desktop/hadoop/Sr_DE_Takehome/csv")

  val flyerOpenUsers = userdata.filter(col("event_type") === "flyer_open")
  // Calculating time spent on each flyer in seconds, as we are interested only in event_type = 'flyer_open'
  val flyerOpenUsersWithduration = flyerOpenUsers.withColumn("time_spent",(col("end_timestamp").cast("long") - col("start_timestamp").cast("long")))
  // Grouping by user_id, flyer_id and merchant_id and calculating count and average to record metrics
  val averageTimeSpentPerFlyer = flyerOpenUsersWithduration.groupBy("user_id","flyer_id","merchant_id").agg(round(avg("time_spent"),2).alias("average_time"),count(lit(1)).alias("count"))
  // Sorting the result by merchant_id and count
  val sortedAverageTimeSpent = averageTimeSpentPerFlyer.sort(col("merchant_id"),col("count").desc)
  // saving the file
  sortedAverageTimeSpent.coalesce(1).write.mode("overwrite").option("header","true").csv("/Users/RAMA/Desktop/hadoop/Sr_DE_Takehome/csv/result")



}
