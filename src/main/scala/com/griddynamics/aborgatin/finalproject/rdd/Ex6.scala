package com.griddynamics.aborgatin.finalproject.rdd

import com.griddynamics.aborgatin.finalproject.{MySQLHelper, NetworkUtils}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object Ex6 {
  def main(args: Array[String]) = {
    var path = "/user/cloudera/events2/*"
    if (args.length == 1) {
      path = args(0)
    }
    //conf
    val sparkConf = new SparkConf()
      .setAppName("Spark final project aborgatin (Exercise 6 - RDD)")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val hadoopConf = sc.hadoopConfiguration
    hadoopConf.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    val dfSimple = sc.textFile(path)
      .map(_.split(","))
      .map(r => (NetworkUtils.ipToNumber(r(4)), r(1))) //(ip_in_number_format, price)

    val blocks = sc.textFile("geo/bl/blocks.csv")
    val headerBlocks = blocks.first()
    val blocksForJoin = blocks.filter(r => r != headerBlocks).map(_.split(",")).map(r => (r(2), r(0)))
    val locations = sc.textFile("geo/lo/locations.csv")
    val headerLocations = locations.first()
    val locationsForJoin = locations.filter(r => r != headerLocations).map(_.split(",")).map(r => (r(0), r(5)))
    val geoJoinedSimple = blocksForJoin
      .join(locationsForJoin)
      .map(r =>
        (NetworkUtils.maskToBound(r._2._1, true), //low ip bound
          NetworkUtils.maskToBound(r._2._1, false), //high ip bound
            r._2._2)) //country name
        .sortBy(_._1)
      .collect()

    val result = dfSimple.map(
      r => (
        NetworkUtils.getCountry(geoJoinedSimple, r._1),
        r._2.trim.toDouble
      )
    )
      .filter(r => r._1 != null && !r._1.equals(""))
      .reduceByKey((a,b) => a + b)
      .sortBy(-_._2)
      .toDF("country", "money_spending")
      .limit(10)


    MySQLHelper.writeToDatabase(result, "ex6_spark_rdd");
//3.381934799E10
//3.381934799E10
  }

}
