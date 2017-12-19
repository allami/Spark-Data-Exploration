package com.allami

import org.apache.spark.sql.SparkSession

import com.allami.Utilities._
import com.allami.Config
object Main extends  App{



  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("data exploration")
    .getOrCreate()

  import spark.implicits._
  val lines = spark.read.text(Config.SparkConf.dataFile).as[String]
  getTopTenServices(lines)
  getTopTenBrowsers(lines)
  WindowPerService(lines)
  getNumberOfConnectionByDay(lines)

}


