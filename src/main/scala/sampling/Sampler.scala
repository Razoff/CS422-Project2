package sampling

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.util.SizeEstimator


// NOTE columns of lineintem
// l_orderkey , l_partkey , l_suppkey , l_linenumber , l_quantity , l_extendedprice , l_discount , l_tax , l_returnflag ,
// l_linestatus , l_shipdate , l_commitdate , l_receiptdate , l_shipinstruct , l_shipmode , l_comment

object Sampler {
  def sample(lineitem: DataFrame, storageBudgetBytes: Long, e: Double, ci: Double): (List[RDD[_]], _) = {
    lineitem.show()

    val key_sample = List(0,1,2)
    val rows = lineitem.rdd

    // QCS is the list of indexes we keep
    def sampling(qcs : List[Int]): (RDD[_])={
      val fractions = rows.map(x => (x(0),x(1),x(2))).distinct().map( x => (x,0.8)).collectAsMap()

      val sample_data = rows.keyBy(x => (x(0), x(1), x(2) )).sampleByKey(false, fractions).map(x => x._2)

      //sample_data.take(15).foreach(println)
      println(sample_data.count())
      println(SizeEstimator.estimate(sample_data.collect()))

      return sample_data
    }


    val ex_rdd = sampling(key_sample)

    return (List(ex_rdd), key_sample)
  }
}
