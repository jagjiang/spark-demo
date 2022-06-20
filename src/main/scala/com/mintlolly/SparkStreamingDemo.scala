package com.mintlolly

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils

/**
 * Created on 2022/5/10
 *
 * @author jiangbo
 *         Description:https://www.jianshu.com/p/30614ff250b5
 */
object SparkStreamingDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkStreamingDemo")
    if(System.getProperty("os.name").toLowerCase().contains("windows")){
      conf.setMaster("local[1]")
    }
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val ssc = new StreamingContext(sc, Seconds(5))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "101.42.251.112:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("windows")

    val value = KafkaUtils.createDirectStream(ssc, PreferConsistent,
      Subscribe[String, String](topics, kafkaParams))
    value.map(_.value()).print()
    ssc.start()
    ssc.awaitTermination()
  }
}
