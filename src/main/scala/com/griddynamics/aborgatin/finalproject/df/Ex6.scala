package com.griddynamics.aborgatin.finalproject.df

import com.griddynamics.aborgatin.finalproject.{MySQLHelper, NetworkUtils}
import com.griddynamics.aborgatin.finalproject.Model.{GeoBlock, GeoLocation, Purchase}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

object Ex6 {




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


    val dfSimple = sc.textFile(path).map(_.split(",")).map(r => Purchase(r(1), NetworkUtils.ipToNumber(r(4)))).toDF()
    val blocks = sc.textFile("geo/bl/blocks.csv")
    val headerBlocks = blocks.first()
    val blocksForJoin = blocks.filter(r => r != headerBlocks).map(_.split(",")).map(r => GeoBlock(r(0), r(2))).toDF()
    val locations = sc.textFile("geo/lo/locations.csv")
    val headerLocations = locations.first()
    val locationsForJoin = locations.filter(r => r != headerLocations).map(_.split(",")).map(r => GeoLocation(r(0), r(5))).toDF()
    val geoJoinedSimple = blocksForJoin
      .join(locationsForJoin, blocksForJoin("countryId") === locationsForJoin("countryId"), "LeftOuter")
      .map(r =>
        (
          NetworkUtils.maskToBound(r.getString(0), isLow = true),
          NetworkUtils.maskToBound(r.getString(0), isLow = false),
          r.getString(3))
      )
      .sortBy(_._1)
      .collect()

    val df1 = dfSimple
      .map(r =>
        (
          NetworkUtils.getCountry(geoJoinedSimple, r.getLong(1)),
          r.getString(0).trim.toDouble)
      )
      .filter(r => r._1 != null && !r._1.equals(""))
      .toDF("country", "money_spending")
    val df2 = df1.groupBy("country").sum("money_spending").sort(desc("sum(money_spending)")).limit(10).toDF("country", "money_spending")
    MySQLHelper.writeToDatabase(df2, "ex6_spark_df");
  }
}