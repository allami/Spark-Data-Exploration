package com.allami

import java.util.Date

import breeze.linalg.sum
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession

import scala.util.matching.Regex
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{coalesce, count, datediff, lag, lit, min, sum}
import org.apache.spark.sql.functions.udf
import scala.util.{Failure, Success, Try}
import com.allami.Utilities._
object Main extends  App{



  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("data exploration")
    .getOrCreate()

  import spark.implicits._
  val lines = spark.read.text("/opt/nginx_access.log").as[String]
  getTopTenServices(lines)
  getTopTenBrowsers(lines)
  WindowPerService(lines)
  getNumberOfConnectionByDay(lines)

}


