package com.bxl.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by root on 2018/3/11.
  */
//class wordcount {}

object wordcount{

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("wc_001")
    val sc = new SparkContext(conf)
    sc.textFile("/user/root/bxl/wc.txt").flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).saveAsTextFile("/user/root/spark_rel/")
  }
}
