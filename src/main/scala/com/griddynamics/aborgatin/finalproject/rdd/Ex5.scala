package com.griddynamics.aborgatin.finalproject.rdd

import com.griddynamics.aborgatin.finalproject.MySQLHelper
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object Ex5 {


  def main(args: Array[String]) = {
    var path = "/user/cloudera/events2/*"
    if (args.length == 1) {
      path = args(0)
    }
    //conf
    val sparkConf = new SparkConf()
      .setAppName("Spark final project aborgatin (Exercise 5 - RDD)")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    val hadoopConf = sc.hadoopConfiguration
    hadoopConf.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    import sqlContext.implicits._

    val events = sc.textFile(path)

    //exercise 1
    println("Top 10 most frequently purchased categories (rdd)")
    val eventsByCat = events.map(line => (line.split(",")(3), 1)).reduceByKey((a, b) => a + b).sortBy(_._2, false)
    MySQLHelper.writeToDatabase(eventsByCat.toDF("category", "count").limit(10), "ex5_1_spark_rdd");

    //exercise 2
    println("Top 10 most frequently purchased product in each category (RDD)")
    val eventsByCatTop10 = events.map(line => (line.split(",")(3), line.split(",")(0), 1))
      .groupBy(_._1)
      .mapValues(
        _.map(r => ((r._1,r._2), r._3))
        .groupBy(_._1)
        .mapValues(_.map(_._2).sum)
        .toSeq
        .sortBy(-_._2)
        .take(10))
        .map(_._2)
        .flatMap(r => r)
        .map(r => (r._1._1, r._1._2, r._2))
    MySQLHelper.writeToDatabase(eventsByCatTop10.toDF("category", "product_name", "count"), "ex5_2_spark_rdd");
  }
}
