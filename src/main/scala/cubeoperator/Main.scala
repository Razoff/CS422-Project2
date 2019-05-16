package cubeoperator

import org.apache.spark.{SparkConf, SparkContext, sql}
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import java.io._

import org.apache.spark
import org.apache.spark.sql.{SQLContext, SparkSession}

object Main {
  def main(args: Array[String]) {
    val reducers = 10

    val inputFile= "/user/cs422-group38/lineorder_small.tbl"
    //val input = new File(getClass.getResource(inputFile).getFile).getPath

    val sparkConf = new SparkConf().setAppName("CS422-Project2_huck") //.setMaster("local[*]")
    val ctx = new SparkContext(sparkConf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(ctx)
    val sqlContext = new SQLContext(ctx)
    //val sa = new spark.sql.SparkSession.Builder

    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", "|")
      .load(inputFile)

    val rdd = df.rdd

    val schema = df.schema.toList.map(x => x.name)

    val dataset = new Dataset(rdd, schema)

    val cb = new CubeOperator(reducers)

    var groupingList = List("lo_suppkey","lo_shipmode","lo_orderdate")

    //val res = cb.cube(dataset, groupingList, "lo_supplycost", "SUM")
    val res = cb.cube(dataset, groupingList, "lo_supplycost", "AVG")


    res.saveAsTextFile("./cube")

    /*
       The above call corresponds to the query:
       SELECT lo_suppkey, lo_shipmode, lo_orderdate, SUM (lo_supplycost)
       FROM LINEORDER
       CUBE BY lo_suppkey, lo_shipmode, lo_orderdate
     */


    //Perform the same query using SparkSQL
    val q1 = df.cube("lo_suppkey","lo_shipmode","lo_orderdate").agg(sum("lo_supplycost") as "sum supplycost")
    q1.show(10,false)


  }
}