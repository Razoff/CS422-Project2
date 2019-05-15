package sampling

import breeze.numerics.sqrt
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.util.SizeEstimator


// NOTE columns of lineintem
// l_orderkey , l_partkey , l_suppkey , l_linenumber , l_quantity , l_extendedprice , l_discount , l_tax , l_returnflag ,
// l_linestatus , l_shipdate , l_commitdate , l_receiptdate , l_shipinstruct , l_shipmode , l_comment

object Sampler {
  def sample(lineitem: DataFrame, storageBudgetBytes: Long, e: Double, ci: Double): (List[RDD[_]], _) = {
    lineitem.show()

    val rows = lineitem.rdd

    val N = rows.count()

    def getZ(): Double = {
      ci match {
        case x if x <= 0.8 => return 1.28
        case x if x <= 0.9 => return 1.64
        case x if x <= 0.95 => return 1.96
        case x if x <= 0.98 => return 2.33
        case x if x <= 0.99 => return 2.58
        case _ => return 2.8
      }
    }

    def getSize(input : RDD[_]): Long = {
      return SizeEstimator.estimate(input.collect())
    }

    val z = getZ()
    println(z)

    //def check_sample(RDD: RDD[_]):

    //val lst : RDD[(Int,List[Double])] = rows.groupBy(x => (x(1), x(2))).map(x => (x._2.size, x._2.map(y => y(5).asInstanceOf[Int].toDouble).toList))
    //val vari = lst.map(x => (x._1, x._2.sum / x._2.length.toDouble, x._2)).map(x => (x._1, x._3.map(y => ((y - x._2) * (y - x._2)) / (x._1 - 1).toDouble ).sum))

    //vari.take(10).foreach(println)

    //mean(xs).flatMap(m => mean(xs.map(x => Math.pow(x-m, 2))))


    //rows.take(10).foreach(println)

    // (orderkey, suppkey, partkey, quantity, shipdate)   P: 0 1 2 4 10
    // (orderkey, suppkey, partkey, quantity)             E
    // (orderkey, suppkey, partkey)                       B
    // (shipdate, orderkey)                               N
    def sample_01(mode : String, prb : Double): RDD[_] = {

      mode match {
        case "P" =>
          val distict_val = rows.map(x => (x(0), x(1), x(2), x(4), x(10))).distinct()
          val fractions = distict_val.map( x => (x,prb)).collectAsMap()
          val sample_data = rows.keyBy(x => (x(0), x(1), x(2), x(4), x(10) )).sampleByKey(false, fractions).map(x => x._2)

          val nh_sh2 = sample_data
            .groupBy(x => (x(0), x(1), x(2), x(4), x(10)))
            .map(x => ((x._1, x._2.size, x._2.map(y => y(5).asInstanceOf[Int].toDouble).toList))) // y(5) is extendedprice
            .map(x => (x._1,x._2, x._3.sum / x._3.length.toDouble, x._3))
            .map(x => (x._1, x._2,x._4.map(y => ((y - x._3) * (y - x._3)) / (x._2).toDouble ).sum))

          val Nh = rows.groupBy(x => (x(0), x(1), x(2), x(4), x(10))).map(x => ((x._1,x._2.size)))

          nh_sh2.take(10).foreach(println)
          println("-------------------")
          Nh.take(10).foreach(println)

          val se = nh_sh2.keyBy(x => x._1).join(Nh.keyBy(x => x._1)).map(x => (x._2._1._2, x._2._1._3, x._2._2._2))

          val error_value = se.map(x => (1 - x._1.toDouble / x._3.toDouble) * (x._3.toDouble / N.toDouble) * (x._3.toDouble / N.toDouble) * (x._2/ x._1.toDouble))

          error_value.take(10).foreach(println)

          if (z * sqrt(error_value.collect().sum) <= e ){
            return sample_data
          }else{
            return null
          }

      }
      return rows
    }

    def sample_02(mode : String): RDD[_] = {
      return rows
    }

    def sample_03(mode : String): RDD[_] = {
      return rows
    }

    def sample_04(mode : String): RDD[_] = {
      return rows
    }

    def sample_05(mode : String): RDD[_] = {
      return rows
    }


    // QCS is the list of indexes we keep
    def sampling(qcs : List[Int]): (RDD[_])={
      val fractions = rows.map(x => (x(0),x(1),x(2))).distinct().map( x => (x,0.8)).collectAsMap()

      val sample_data = rows.keyBy(x => (x(0), x(1), x(2) )).sampleByKey(false, fractions).map(x => x._2)

      //sample_data.take(15).foreach(println)
      println(sample_data.count())
      println(SizeEstimator.estimate(sample_data.collect()))

      return sample_data
    }

    val rr = sample_01("P", 0.2)
    //val ex_rdd = sampling(key_sample)

    //return (List(ex_rdd), key_sample)
    return null
  }
}
