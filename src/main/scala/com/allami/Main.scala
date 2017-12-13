package com.allami

import java.text.SimpleDateFormat
import java.util.Date

import breeze.linalg.sum
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession

import scala.util.matching.Regex
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{coalesce, count, datediff, lag, lit, min, sum}
import org.apache.spark.sql.functions.udf
import scala.util.{Failure, Success, Try}

object Main extends  App{


  def getTopTenServices(lines:Dataset[String]){
    import spark.implicits._
    val services = new Regex("(\\/service/([a-zA-Z0-9])*\\/)")
    val result=lines.map{ line=>
      import spark.implicits._
      val data=line.split("\"")(1)
      (services findAllIn data).mkString(",")
    }
    result.withColumnRenamed("value", "service").filter("service!=''").groupBy("service").count().sort($"count".desc).show()

  }
  def getDate(s: String) : Option[Long] = s match {
    case "" => None
    case _ => {
      val format = new java.text.SimpleDateFormat("yyyy/MMM/dd")

      Try(format.parse(s).getTime) match {
        case Success(t) => Some(t)
        case Failure(_) => None
      }
    }
  }
  def getTopTenBrowsers(lines:Dataset[String]): Unit ={
    import spark.implicits._
    val navigators=lines.map{ line=>
      val data=line.split("\"")(5)
      data
    }
    navigators.withColumnRenamed("value", "navigator").filter("navigator!='-'").groupBy("navigator").count().sort($"count".desc).show()
  }


  val toDate = (s: String) => {
    val format = new java.text.SimpleDateFormat("dd/MMM/yyyy")
   val dt:String=format.parse(s).getTime().toString()
    dt
  }

  def getNumberOfConnectionByDay(lines:Dataset[String]){

    spark.udf.register("toDate", toDate)
    val dd= new Regex("\\[(.*?)\\]")
    import spark.implicits._
    val result=lines.map{ line=>
      ( ( dd findAllIn line).mkString(",").split(":")(0)).replace("[","")
    }.registerTempTable("connections")
    spark.sqlContext.sql("select count(*) AS total from connections GROUP BY toDate(value) ORDER BY total desc").show(100)
  }
  def WindowPerService(lines:Dataset[String]): Unit ={
    val services = new Regex("(\\/service/([a-zA-Z0-9])*\\/)")
    import spark.implicits._
    import org.apache.spark.sql.expressions.Window
    val service = Window.partitionBy("service")

    val windowing=lines.map{ line=>
      import spark.implicits._
      val navig=line.split("\"")(5)
      val service=line.split("\"")(1)
      (services findAllIn service).mkString(",")
      (navig,service)
    }
    windowing.withColumnRenamed("_1","navig").withColumnRenamed("_2","service").select('service,'navig,count("service").over(service)).show

  }

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("data exploration")
    .getOrCreate()

  import spark.implicits._
  val lines = spark.read.text("/opt/nginx_access.log").as[String]
  getTopTenServices(lines)
  getTopTenBrowsers(lines)
  getNumberOfConnectionByDay(lines)
  WindowPerService(lines)

}


