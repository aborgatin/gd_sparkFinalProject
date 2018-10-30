package com.griddynamics.aborgatin.finalproject.sql

import com.griddynamics.aborgatin.finalproject.MySQLHelper
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

object Ex5 {

  case class Purchase(productName: String, price: String, date: String, category: String, ipAddress: String)

  def main(args: Array[String]) = {
    //conf
    val sparkConf = new SparkConf().setAppName("Spark final project aborgatin (Exercise 5 - SQL)")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    //export jdbc conf
    val url = MySQLHelper.getUrl
    val prop = MySQLHelper.getProperties


    val df = sc.textFile("events2/*").map(_.split(",")).map(r => Purchase(r(0), r(1), r(2), r(3), r(4))).toDF()
    df.registerTempTable("purchase")

    //exercise 1
    println("Top 10 most frequently purchased categories (SQL)")
    val queryEx1 = "select category, count(*) c FROM purchase group by category order by c desc limit 10"
    sqlContext.sql(queryEx1).write.jdbc(url, "ex5_1_spark_sql", prop)


    //exercise 2
    println("Top 10 most frequently purchased product in each category (SQL)")
    sqlContext.sql("select category, productName, c from (select category, productName, c, row_number() over(partition by category order by c desc) n from (select category, productName, count(*) c FROM purchase group by productName, category) A) B WHERE n <= 10")
      .write.jdbc(url, "ex5_2_spark_sql", prop)

    sc.stop()
  }


}
