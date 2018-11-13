package com.griddynamics.aborgatin.finalproject.df

import com.griddynamics.aborgatin.finalproject.MySQLHelper
import org.apache.commons.net.util.SubnetUtils
import org.apache.spark.sql.{SQLContext, SaveMode}
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
    var path = "events2/*"
    if (args.length == 1) {
      path = args(0)
    }

    val sparkConf = new SparkConf().setAppName("Spark final project aborgatin (Exercise 6 - DataFrame)")
    val sc = new SparkContext(sparkConf);
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val hadoopConf = sc.hadoopConfiguration
    hadoopConf.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    //export jdbc conf
    val url = MySQLHelper.getUrl
    val prop = MySQLHelper.getProperties
    val dfSimple = sc.textFile(path).map(_.split(",")).map(r => Purchase(r(1), r(4))).toDF()
    val blocks = sc.textFile("geo/blocks.csv")
    val headerBlocks = blocks.first()
    val blocksForJoin = blocks.filter(r => r != headerBlocks).map(_.split(",")).map(r => GeoBlock(r(0), r(1))).toDF()
    val locations = sc.textFile("geo/locations.csv")
    val headerLocations = locations.first()
    val locationsForJoin = locations.filter(r => r != headerLocations).map(_.split(",")).map(r => GeoLocation(r(0), r(5))).toDF()
    val geoJoinedSimple = blocksForJoin.join(locationsForJoin, blocksForJoin("countryId") === locationsForJoin("countryId"), "LeftOuter").map(r => GeoJoined(r.getString(0), r.getString(3))).toDF()
    val applyMaskUdf = org.apache.spark.sql.functions.udf((ip: String, mask: String) => applyMask(ip, mask))
    val df1 = dfSimple.join(broadcast(geoJoinedSimple), applyMaskUdf(dfSimple("ipAddress"), geoJoinedSimple("ipMask")), "LeftOuter")
    val df2 = df1.map(r => (r.getString(3), r.getString(0).trim.toFloat)).filter(_._1 != null).toDF("country", "money_spending")
    val df3 = df2.groupBy("country").sum("money_spending").sort(desc("sum(money_spending)")).limit(10).toDF("country", "money_spending")
    df3.toDF().write.mode(SaveMode.Overwrite).jdbc(url, "ex6_spark_df", prop)

  }
}