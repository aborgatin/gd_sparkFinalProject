package com.griddynamics.aborgatin.finalproject.rdd

import com.griddynamics.aborgatin.finalproject.MySQLHelper
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

object Ex5 {


  def main(args: Array[String]) = {
    var path = "/user/aborgatin/events/*"
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

    //export jdbc conf
    val url = MySQLHelper.getUrl
    val prop = MySQLHelper.getProperties

    val events = sc.textFile(path)

    //exercise 1
    println("Top 10 most frequently purchased categories (rdd)")
    val eventsByCat = events.map(line => (line.split(",")(3), 1)).reduceByKey((a, b) => a + b).sortBy(_._2, false)
    eventsByCat.toDF("category", "count").limit(10).write.mode(SaveMode.Overwrite).jdbc(url, "ex5_1_spark_rdd", prop)

    //exercise 2
    println("Top 10 most frequently purchased product in each category (RDD)")
    val eventsByCatTop10 = events.map(line => ((line.split(",")(3), line.split(",")(0)), 1)).
      reduceByKey((a, b) => a + b).map(r => (r._1._1, (r._1._2, r._2))).
      groupByKey().map({ case (key, numbers) => key -> numbers.toList.
      sortBy(-_._2).take(10).map(r => (key,r._1, r._2))
    }).map(_._2).flatMap( r => r)
    eventsByCatTop10.toDF("category", "product_name", "count").write.mode(SaveMode.Overwrite).jdbc(url, "ex5_2_spark_rdd", prop)
  }


}
