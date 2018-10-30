package com.griddynamics.aborgatin.finalproject

object MySQLHelper {
  private val url = "jdbc:mysql://localhost:3306/ab"
  private val prop = new java.util.Properties
  prop.setProperty("driver", "com.mysql.jdbc.Driver")
  prop.setProperty("user", "root")
  prop.setProperty("password", "cloudera")

  def getUrl: String ={
    url
  }

  def getProperties: java.util.Properties = {
    prop
  }


}
