package sampling

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import java.util.Random

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.random.RandomRDDs
import org.apache.spark.sql.types._
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.{Row, SparkSession}

object Executor {

  //|l_orderkey|l_partkey|l_suppkey|l_linenumber|l_quantity|l_extendedprice|l_discount|l_tax|l_returnflag|l_linestatus|l_shipdate|l_commitdate|l_receiptdate|l_shipinstruct|l_shipmode| l_comment|

  def select_sample(desc : Description, prefered_sample : List[Int]): RDD[_]={
    val sample_list : List[List[Int]] = desc.sampleDescription.asInstanceOf[List[List[Int]]]
    if (sample_list.length == 0){
      return desc.samples(0)
    } else if (sample_list.contains(prefered_sample)){
      return desc.samples(sample_list.indexOf(prefered_sample))
    }else{ // If we did not get our favourite sample just pick at random
      val rand = new Random(System.currentTimeMillis())
      val random_index = rand.nextInt(sample_list.length)
      return desc.samples(random_index)
    }
  }

  def execute_Q1(desc: Description, session: SparkSession, params: List[Any]) = {
    assert(params.size == 1)
    val p1 :String = params(0).asInstanceOf[String]

    val lineitem : DataFrame = session.createDataFrame(select_sample(desc, List(1,2,3))
      .map(x => Row(x))
      .map(x  => x.get(0).asInstanceOf[Row]), desc.lineitem.schema)

    lineitem.createOrReplaceTempView("lineitem")


    session.sql(
      "select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty , sum(l_extendedprice) as sum_base_price, sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order from lineitem where l_shipdate <= date_sub(date('1998-12-01'), " + p1 + ") group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus"
    ).show()
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
    assert(params.size == 2)

    val n_name : String = params(0).asInstanceOf[String]
    val sec : String = params(1).asInstanceOf[String]

    desc.partsupp.createOrReplaceTempView("partsupp")
    desc.supplier.createOrReplaceTempView("supplier")
    desc.nation.createOrReplaceTempView("nation")

    val subquery: String = session.sql(
      "SELECT  sum(ps_supplycost * ps_availqty) * " + sec + " FROM partsupp, supplier, nation WHERE ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = '"+ n_name +"' "
    ).collect()(0).get(0).asInstanceOf[java.math.BigDecimal].toString

    session.sql(
      "SELECT ps_partkey, (sum(ps_supplycost * ps_availqty)) as agre FROM partsupp, supplier, nation WHERE ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = '"+ n_name +"' GROUP BY ps_partkey HAVING sum(ps_supplycost * ps_availqty) > " + subquery + " ORDER BY agre "
    ).show()

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
