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

  def get_lineitem_df (desc : Description, session : SparkSession ,prefered_sample : List[Int]):DataFrame = {
    return session.createDataFrame(select_sample(desc, List(1,2,3))
      .map(x => Row(x))
      .map(x  => x.get(0).asInstanceOf[Row]), desc.lineitem.schema)
  }

  def execute_Q1(desc: Description, session: SparkSession, params: List[Any]) = {
    assert(params.size == 1)
    val p1 :String = params(0).asInstanceOf[String]

    get_lineitem_df(desc, session, List(1,2,3)).createOrReplaceTempView("lineitem")

    session.sql(
      "select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty , sum(l_extendedprice) as sum_base_price, sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order " +
        "from lineitem " +
        "where l_shipdate <= date_sub(date('1998-12-01'), " +
        p1 +
        ") group by l_returnflag, l_linestatus " +
        "order by l_returnflag, l_linestatus"
    ).show()
  }

  def execute_Q3(desc: Description, session: SparkSession, params: List[Any]) = {
    assert(params.size == 2)

    val mktseg : String = params(0).asInstanceOf[String]
    val date : String = params(1).asInstanceOf[String]

    get_lineitem_df(desc, session, List(1,2,3)).createOrReplaceTempView("lineitem")
    desc.orders.createOrReplaceTempView("orders")
    desc.customer.createOrReplaceTempView("customer")

    session.sql(
      "select l_orderkey, sum(l_extendedprice * (1 - l_discount)) as revenue, o_orderdate, o_shippriority " +
        "from customer, orders, lineitem " +
        "where c_mktsegment = '" + mktseg + "' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate < date('" +
        date +
        "') and l_shipdate > date('" +
        date +
        "') group by l_orderkey, o_orderdate, o_shippriority order by revenue desc, o_orderdate"
    ).show()

  }

  def execute_Q5(desc: Description, session: SparkSession, params: List[Any]) = {
    assert(params.size == 2)

    val name : String = params(0).asInstanceOf[String]
    val date : String = params(1).asInstanceOf[String]

    get_lineitem_df(desc, session, List(1,2,3)).createOrReplaceTempView("lineitem")
    desc.orders.createOrReplaceTempView("orders")
    desc.customer.createOrReplaceTempView("customer")
    desc.supplier.createOrReplaceTempView("supplier")
    desc.nation.createOrReplaceTempView("nation")
    desc.region.createOrReplaceTempView("region")

    session.sql(
      "select n_name, sum(l_extendedprice * (1 - l_discount)) as revenue " +
        "from customer, orders, lineitem, supplier, nation, region " +
        "where c_custkey = o_custkey and l_orderkey = o_orderkey and l_suppkey = s_suppkey and c_nationkey = s_nationkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = '" +
        name +
        "' and o_orderdate >= date('" +
        date +
        "')and o_orderdate < add_months(date('" +
        date +
        "'), 12) group by n_name order by revenue desc"
    ).show()
  }

  def execute_Q6(desc: Description, session: SparkSession, params: List[Any]) = {
    assert(params.size == 3)

    val date : String = params(0).asInstanceOf[String]
    val disc : String = params(1).asInstanceOf[String]
    val quan : String = params(2).asInstanceOf[String]

    get_lineitem_df(desc, session, List(1,2,3)).createOrReplaceTempView("lineitem")

    session.sql(
      "select sum(l_extendedprice * l_discount) as revenue " +
        "from lineitem " +
        "where l_shipdate >= date('" +
        date +
        "')  and l_shipdate < add_months(date('" +
        date +
        "'),12) and l_discount between " +
        disc +
        " - 0.01 and " +
        disc +
        " + 0.01 and l_quantity < " + quan + " "
    ).show()

  }

  def execute_Q7(desc: Description, session: SparkSession, params: List[Any]) = {
    assert(params.size == 2)

    val name1 : String = params(0).asInstanceOf[String]
    val name2 : String = params(1).asInstanceOf[String]

    get_lineitem_df(desc, session, List(1,2,3)).createOrReplaceTempView("lineitem")
    desc.supplier.createOrReplaceTempView("supplier")
    desc.orders.createOrReplaceTempView("orders")
    desc.customer.createOrReplaceTempView("customer")
    desc.nation.createOrReplaceTempView("nation")

    session.sql(
      "select n1.n_name as supp_nation, n2.n_name as cust_nation, year(l_shipdate) as l_year, l_extendedprice * (1 - l_discount) as volume " +
        "from supplier, lineitem, orders, customer, nation n1, nation n2 " +
        "where s_suppkey = l_suppkey and o_orderkey = l_orderkey and c_custkey = o_custkey and s_nationkey = n1.n_nationkey and c_nationkey = n2.n_nationkey and ( (n1.n_name = '" +
        name1 +
        "' and n2.n_name = '" +
        name2 +
        "') or (n1.n_name = '" +
        name2 +
        "' and n2.n_name = '" +
        name1 +
        "') ) and l_shipdate between date('1995-01-01') and date('1996-12-31')"
    ).createOrReplaceTempView("shipping")

    session.sql(
      "select supp_nation, cust_nation, l_year, sum(volume) as revenue " +
        "from shipping " +
        "group by supp_nation, cust_nation, l_year " +
        "order by supp_nation, cust_nation, l_year "
    ).show()
  }

  def execute_Q9(desc: Description, session: SparkSession, params: List[Any]) = {
    assert(params.size == 1)

    val like : String = params(0).asInstanceOf[String]

    get_lineitem_df(desc, session, List(1,2,3)).createOrReplaceTempView("lineitem")
    desc.part.createOrReplaceTempView("part")
    desc.supplier.createOrReplaceTempView("supplier")
    desc.partsupp.createOrReplaceTempView("partsupp")
    desc.orders.createOrReplaceTempView("orders")
    desc.nation.createOrReplaceTempView("nation")

    session.sql(
      "select n_name as nation, year(o_orderdate) as o_year, l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount " +
        "from part, supplier, lineitem, partsupp, orders, nation " +
        "where s_suppkey = l_suppkey and ps_suppkey = l_suppkey and ps_partkey = l_partkey and p_partkey = l_partkey and o_orderkey = l_orderkey and s_nationkey = n_nationkey and p_name like '%" + like + "%'"
    ).createOrReplaceTempView("profit")

    session.sql(
      "select nation, o_year, sum(amount) as sum_profit " +
        "from profit " +
        "group by nation, o_year " +
        "order by nation, o_year desc"
    ).show()
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
