name := "spark-final-project"

version := "0.2"

scalaVersion := "2.10.5"

val sparkVersion = "1.6.0"
//val sparkVersion = "2.1.0"

libraryDependencies += "org.scala-lang" % "scala-library" % "2.10.5"
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-hive" % sparkVersion
libraryDependencies +=  "org.apache.hadoop" % "hadoop-client" % "2.6.0"
libraryDependencies +=  "commons-net" % "commons-net" % "3.6"


