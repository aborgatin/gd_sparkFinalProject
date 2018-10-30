package com.griddynamics.aborgatin.finalproject.df

import org.apache.spark.{SparkConf, SparkContext}

object Example {
  val sparkConf = new SparkConf().setAppName("Spark final project aborgatin (Exercise 5 - DataFrame)")
  val sc = new SparkContext(sparkConf)

  val stringList = Array("Spark is awesome","Spark is cool")
  val stringRDD = sc.parallelize(stringList).collect()

}
