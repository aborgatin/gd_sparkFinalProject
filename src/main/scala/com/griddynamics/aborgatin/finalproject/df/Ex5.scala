package com.griddynamics.aborgatin.finalproject.df

import com.griddynamics.aborgatin.finalproject.MySQLHelper
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel

object Ex5 {

  case class Purchase(productName: String, price: String, date: String, category: String, ipAddress: String)

  def main(args: Array[String]) = {
    //conf
    val sparkConf = new SparkConf().setAppName("Spark final project aborgatin (Exercise 5 - DataFrame)")

    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    import sqlContext.implicits._
    val hadoopConf = sc.hadoopConfiguration
    hadoopConf.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    //export jdbc conf
    val url = MySQLHelper.getUrl
    val prop = MySQLHelper.getProperties


    val df = sc.textFile("/user/aborgatin/events2/*").map(_.split(",")).map(r => Purchase(r(0), r(1), r(2), r(3), r(4))).toDF()
    df.persist(StorageLevel.MEMORY_AND_DISK_SER)
    //exercise 1
    println("Top 10 most frequently purchased categories (df)")
    val dfGroupByCat = df.groupBy("category").count().sort(desc("count")).limit(10)
    dfGroupByCat.persist(StorageLevel.MEMORY_AND_DISK_SER).write.jdbc(url, "ex5_1_spark_df", prop)


    //exercise 2
    println("Top 10 most frequently purchased product in each category (DF)")
    val w = Window.partitionBy($"category").orderBy($"count".desc)
    val dfGroupByCatAndName = df.groupBy("category", "productName").count().sort(asc("category"), desc("count")).withColumn("rn", row_number().over(w)).where("rn <= 10").drop("rn")
    dfGroupByCatAndName.persist(StorageLevel.MEMORY_AND_DISK_SER).write.jdbc(url, "ex5_2_spark_df", prop)
    sc.stop()
  }


}
