package com.griddynamics.aborgatin.finalproject.df

import com.griddynamics.aborgatin.finalproject.MySQLHelper
import org.apache.commons.net.util.SubnetUtils
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

object Ex6 {

  def applyMask(ip: String, mask: String): Boolean = {
    val utils = new SubnetUtils(mask)
    val info = utils.getInfo
    return info.isInRange(ip)
  }

  case class Geo(ipMask: String, countryId: String, countryName: String)

  case class Purchase(price: String, ipAddress: String)

  case class GeoBlock(ipMask: String, countryId: String)

  case class GeoLocation(countryId: String, countryName: String)

  case class GeoJoined(ipMask: String, countryName: String)

  def main(args: Array[String]) = {
    val sparkConf = new SparkConf().setAppName("Spark final project aborgatin (Exercise 6 - DataFrame)")
    val sc = new SparkContext(sparkConf);
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val hadoopConf = sc.hadoopConfiguration
    hadoopConf.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    //export jdbc conf
    val url = MySQLHelper.getUrl
    val prop = MySQLHelper.getProperties

    println("Creating DataFrame Purchases")
    val dfSimple = sc.textFile("events2/*").map(_.split(",")).map(r => Purchase(r(1), r(4))).toDF()

    println("Creating DataFrame Blocks")
    val blocks = sc.textFile("geo/blocks.csv")
    val headerBlocks = blocks.first()
    val blocksForJoin = blocks.filter(r => r != headerBlocks).map(_.split(",")).map(r => GeoBlock(r(0), r(1))).toDF()

    println("Creating DataFrame Locations")
    val locations = sc.textFile("geo/locations.csv")
    val headerLocations = locations.first()
    val locationsForJoin = locations.filter(r => r != headerLocations).map(_.split(",")).map(r => GeoLocation(r(0), r(5))).toDF()

    println("Creating DataFrame GeoJoin")
    val geoJoinedSimple = blocksForJoin.join(locationsForJoin, blocksForJoin("countryId") === locationsForJoin("countryId"), "LeftOuter").map(r => GeoJoined(r.getString(0), r.getString(3))).toDF()

    println("Creating UDF function")
    val applyMaskUdf = org.apache.spark.sql.functions.udf((ip: String, mask: String) => applyMask(ip, mask))

    println("Join purchases and geo by UDF function")
    val df1 = dfSimple.join(geoJoinedSimple, applyMaskUdf(dfSimple("ipAddress"), geoJoinedSimple("ipMask")), "LeftOuter")

    println("Calculation top 10 countries with the highest money spending")
    val df2 = df1.map(r => (r.getString(3), r.getString(0).trim.toFloat)).toDF("country", "money_spending")
    val df3 = df2.groupBy("country").sum("money_spending").sort(desc("sum(money_spending)")).limit(10).toDF("country", "money_spending")
    df3.toDF().write.jdbc(url, "ex6_spark_df", prop)

  }
}