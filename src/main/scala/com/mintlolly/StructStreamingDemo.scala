package com.mintlolly

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
 * Created on 2022/5/10
 *
 * @author jiangbo
 *         Description:
 */
object StructStreamingDemo {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("log").setLevel(Level.ERROR)
    val ss = SparkSession.builder().appName("StructStreamingDemo")
    if(System.getProperty("os.name").toLowerCase().contains("windows")){
      ss.master("local[*]")
    }
    val session = ss.getOrCreate()
    import session.implicits._

    val df = session.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "101.42.251.112:9092")
      .option("subscribe", "windows")
      .option("startingOffsets", "earliest")
      .load()
      .select("value")
      .as[String]
    val value = df.map(f =>{
      val strings = f.split(",")
      (strings(0),strings(1).toInt,strings(2))
    }).toDF("name","age","timestamp")

    val frame = value.groupBy("name").max("age")


    val query = frame
      .writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()
  }
}
