package com.griddynamics.aborgatin.finalproject.rdd

import com.griddynamics.aborgatin.finalproject.MySQLHelper
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object Ex5 {

  case class Purchase(productName: String, price: String, date: String, category: String, ipAddress: String)

  def main(args: Array[String]) = {
    //conf
    val sparkConf = new SparkConf().setAppName("Spark final project aborgatin (Exercise 5 - RDD)")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    //export jdbc conf
    val url = MySQLHelper.getUrl
    val prop = MySQLHelper.getProperties

    val events = sc.textFile("events2/*")
    events.persist(StorageLevel.DISK_ONLY)

    //exercise 1
    println("Top 10 most frequently purchased categories (rdd)")
    val eventsByCat = events.map(line => (line.split(",")(3), 1)).reduceByKey((a, b) => a + b).sortBy(_._2, false)
    eventsByCat.toDF("category", "count").write.jdbc(url, "ex5_1_spark_rdd", prop)

    //exercise 2
    println("Top 10 most frequently purchased product in each category (RDD)")
    val eventsByCatTop10 = events.map(line => ((line.split(",")(3), line.split(",")(0)), 1)).
      reduceByKey((a, b) => a + b).map(r => (r._1._1, (r._1._2, r._2))).
      groupByKey().map({ case (key, numbers) => key -> numbers.toList.
      sortBy(-_._2).take(10).map(r => (key,r._1, r._2))
    }).map(_._2).flatMap( r => r)
    eventsByCatTop10.toDF("category", "product_name", "count").write.jdbc(url, "ex5_2_spark_rdd", prop)

    sc.stop()
  }


}
