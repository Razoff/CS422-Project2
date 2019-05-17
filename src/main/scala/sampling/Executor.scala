package sampling

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import java.util.Random

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.random.RandomRDDs
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

object Executor {

  //|l_orderkey|l_partkey|l_suppkey|l_linenumber|l_quantity|l_extendedprice|l_discount|l_tax|l_returnflag|l_linestatus|l_shipdate|l_commitdate|l_receiptdate|l_shipinstruct|l_shipmode| l_comment|


  val schema : StructType = StructType(
    StructField("l_orderkey", IntegerType, false) ::
      StructField("l_partkey", IntegerType, false) ::
      StructField("l_suppkey", IntegerType, false) ::
      StructField("l_linenumber", IntegerType, false) ::
      StructField("l_quantity", DoubleType, false) ::
      StructField("l_extendedprice", DoubleType, false) ::
      StructField("l_discount", DoubleType, false) ::
      StructField("l_tax", DoubleType, false) ::
      StructField("l_returnflag", StringType, false) ::
      StructField("l_linestatus", StringType, false) ::
      StructField("l_shipdate", DateType, false) ::
      StructField("l_commitdate", DateType, false) ::
      StructField("l_receiptdate", DateType, false) ::
      StructField("l_shipinstruct", StringType, false) ::
      StructField("l_shipmode", StringType, false) ::
      StructField("l_comment", StringType, false) ::
      Nil)

  def select_sample(desc : Description, prefered_sample : List[Int]): RDD[_]={
    val sample_list : List[List[Int]] = desc.sampleDescription.asInstanceOf[List[List[Int]]]
    if (sample_list.contains(prefered_sample)){
      return desc.samples(sample_list.indexOf(prefered_sample))

    }else{ // If we did not get our favourite sample just pick at random
      val rand = new Random(System.currentTimeMillis())
      val random_index = rand.nextInt(sample_list.length)
      return desc.samples(random_index)
    }
  }

  def execute_Q1(desc: Description, session: SparkSession, params: List[Any]) = {
    assert(params.size == 1)
    // For example, for Q1, params(0) is the interval from the where close

    val lineitem = session.createDataFrame(select_sample(desc, List(1,2,3)).map(x => Row(x)), schema)

    lineitem.createOrReplaceTempView("lineitem")
    


  }

  def execute_Q3(desc: Description, session: SparkSession, params: List[Any]) = {
    // TODO: implement
    assert(params.size == 2)
    // https://github.com/electrum/tpch-dbgen/blob/master/queries/3.sql
    // using:
    // params(0) as :1
    // params(1) as :2
  }

  def execute_Q5(desc: Description, session: SparkSession, params: List[Any]) = {
    // TODO: implement
  }

  def execute_Q6(desc: Description, session: SparkSession, params: List[Any]) = {
    // TODO: implement
  }

  def execute_Q7(desc: Description, session: SparkSession, params: List[Any]) = {
    // TODO: implement
  }

  def execute_Q9(desc: Description, session: SparkSession, params: List[Any]) = {
    // TODO: implement
  }

  def execute_Q10(desc: Description, session: SparkSession, params: List[Any]) = {
    // TODO: implement
  }

  def execute_Q11(desc: Description, session: SparkSession, params: List[Any]) = {
    // TODO: implement
  }

  def execute_Q12(desc: Description, session: SparkSession, params: List[Any]) = {
    // TODO: implement
  }

  def execute_Q17(desc: Description, session: SparkSession, params: List[Any]) = {
    // TODO: implement
  }

  def execute_Q18(desc: Description, session: SparkSession, params: List[Any]) = {
    // TODO: implement
  }

  def execute_Q19(desc: Description, session: SparkSession, params: List[Any]) = {
    // TODO: implement
  }

  def execute_Q20(desc: Description, session: SparkSession, params: List[Any]) = {
    // TODO: implement
  }
}
