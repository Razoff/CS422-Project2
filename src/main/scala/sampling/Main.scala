package sampling

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.random.RandomRDDs
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

import java.io._

object Main {
  def main(args: Array[String]) {

    val inputFile= "../lineorder_small.tbl"
    //val inputFile= "/user/cs422-group38/lineorder_small.tbl"


    val input = new File(getClass.getResource(inputFile).getFile).getPath

    val conf = new SparkConf().setAppName("huck").setMaster("local[*]")
    val sc = SparkContext.getOrCreate(conf)
    val session = SparkSession.builder().getOrCreate();

    val df = session.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", "|")
      //.load(inputFile)
      .load(input)

    /*val rdd = RandomRDDs.uniformRDD(sc, 100000)
    val rdd2 = rdd.map(f => Row.fromSeq(Seq(f * 2, (f*10).toInt)))

    rdd2.take(10).foreach(println)

    val r2d2 = rdd2.map(x => Row.fromTuple(x.get(0), x.get(1).asInstanceOf[Double].toInt))

    r2d2.take(10).foreach(println)

    val table = session.createDataFrame(r2d2, StructType(
      StructField("A1", DoubleType, false) ::
      StructField("A2", IntegerType, false) ::
      Nil
    ))*/

    var desc = new Description
    //desc.lineitem = table
    desc.e = 0.1
    desc.ci = 0.95


    val path_to_data = "./tpch_parquet_sf1/"
    //val path_to_data = "/cs422-data/tpch/sf100/parquet/"

    desc.customer = session.read.parquet(path_to_data + "customer.parquet")
    //desc.lineitem = df
    desc.lineitem = session.read.parquet(path_to_data + "lineitem_small.parquet")
    desc.nation = session.read.parquet(path_to_data + "nation.parquet")
    desc.orders = session.read.parquet(path_to_data + "order.parquet")
    desc.part = session.read.parquet(path_to_data + "part.parquet")
    desc.partsupp = session.read.parquet(path_to_data + "partsupp.parquet")
    desc.region = session.read.parquet(path_to_data + "region.parquet")
    desc.supplier = session.read.parquet(path_to_data + "supplier.parquet")

    //desc.lineitem.show()


    //val tmp = Sampler.sample(desc.lineitem.sample(false, 0.3).toDF(), 10000000, desc.e, desc.ci)
    //desc.samples = tmp._1
    //desc.sampleDescription = tmp._2

    desc.samples = List(desc.lineitem.sample(false, 0.2).rdd)
    desc.sampleDescription = List()

    // check storage usage for samples

    // Execute first query

    //desc.nation.show()
    //desc.partsupp.show()
    //desc.customer.show()
    //desc.region.show()

    //Executor.execute_Q1(desc, session, List("3"))
    //Executor.execute_Q3(desc, session, List("AUTOMOBILE","1998-12-01"))
    //Executor.execute_Q5(desc, session, List("EUROPE", "1998-12-01"))
    //Executor.execute_Q6(desc, session, List("1998-12-01", "0.05", "100"))
    //Executor.execute_Q7(desc, session, List("INDIA", "IRAN"))
    Executor.execute_Q9(desc, session, List("a"))
    //Executor.execute_Q11(desc, session, List("CANADA", "0.1"))
  }     
}
