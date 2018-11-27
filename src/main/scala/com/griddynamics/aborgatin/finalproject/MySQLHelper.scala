package com.griddynamics.aborgatin.finalproject

import org.apache.spark.sql.{DataFrame, SaveMode}

object MySQLHelper {
  private val url = "jdbc:mysql://localhost:3306/ab"
//  private val url = "jdbc:mysql://10.0.0.21:3306/ab"
  private val prop = new java.util.Properties
  prop.setProperty("driver", "com.mysql.jdbc.Driver")
  prop.setProperty("user", "root")
  prop.setProperty("password", "cloudera")

  def writeToDatabase(df: DataFrame, table: String): Unit = {
    df.toDF().write.mode(SaveMode.Overwrite).jdbc(url, table, prop)
  }


}
