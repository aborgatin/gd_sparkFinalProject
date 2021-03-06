package com.griddynamics.aborgatin.finalproject.sql

import com.griddynamics.aborgatin.finalproject.MySQLHelper
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object Ex5 {

  case class Purchase(productName: String, price: String, date: String, category: String, ipAddress: String)

  def main(args: Array[String]) = {
    var path = "/user/aborgatin/events/*"
    if (args.length == 1) {
      path = args(0)
    }
    //conf
    val sparkConf = new SparkConf().setAppName("Spark final project aborgatin (Exercise 5 - SQL)")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    val hadoopConf = sc.hadoopConfiguration
    hadoopConf.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    import sqlContext.implicits._

    val df = sc.textFile(path).map(_.split(",")).map(r => Purchase(r(0), r(1), r(2), r(3), r(4))).toDF()
    df.registerTempTable("purchase")

    //exercise 1
    println("Top 10 most frequently purchased categories (SQL)")
    val queryEx1 =
      "select category, count(*) c " +
      "from purchase " +
        "group by category " +
        "order by c desc " +
        "limit 10"
    MySQLHelper.writeToDatabase(sqlContext.sql(queryEx1), "ex5_1_spark_sql");

    //exercise 2
    println("Top 10 most frequently purchased product in each category (SQL)")
    val queryEx2 =
      "select category, productName, c " +
        "from " +
          "(select category, productName, c, row_number() over(partition by category order by c desc) n " +
           "from " +
            "(" +
              "select category, productName, count(*) c " +
              "FROM purchase " +
              "group by productName, category" +
            ") A" +
          ") B " +
        "WHERE n <= 10"
    MySQLHelper.writeToDatabase(sqlContext.sql(queryEx2), "ex5_2_spark_sql");
  }
}
