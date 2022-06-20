package com.mintlolly

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created on 2022/5/9
 *
 * @author jiangbo
 *         Description:
 */
object SparkDemo {
  def main(args: Array[String]): Unit = {
    //创建执行环境
    val conf = new SparkConf().setAppName("SparkDemo")
    System.getProperty("os.name")
    if(System.getProperty("os.name").toLowerCase.contains("windows")){
      conf.setMaster("local[*]");
    }
    val input = args(0)
    val output = args(1)
    val check = args(2)
    val fs = FileSystem.get(new Configuration())
    if(fs.exists(new Path(output))){
      fs.delete(new Path(output),true)
    }
    val sc = new SparkContext(conf)

    val score = sc.textFile(input)
    //根据姓名分组，并将数据进行标号
    val flatMapAfterGroupBy = score.groupBy(f => {
      f.split(",")(0)
    }).map(f => {
      var i = 0;
      f._2.map(f => {
        val a = i + "," + f
        i = i + 1
        a
      })
    }).flatMap(f=>f)
    sc.setCheckpointDir(check)
    flatMapAfterGroupBy.checkpoint()
    flatMapAfterGroupBy.map(f =>{
      val scores = f.split(",")
      f + "," + scores(2)+scores(3)+scores(4)
    }).saveAsTextFile(output)
  }
}
