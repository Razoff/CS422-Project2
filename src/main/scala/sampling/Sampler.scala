package sampling

import breeze.numerics.sqrt
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.util.SizeEstimator


// NOTE columns of lineintem
// l_orderkey , l_partkey , l_suppkey , l_linenumber , l_quantity , l_extendedprice , l_discount , l_tax , l_returnflag ,
// l_linestatus , l_shipdate , l_commitdate , l_receiptdate , l_shipinstruct , l_shipmode , l_comment

// |l_orderkey|l_partkey|l_suppkey|l_linenumber|l_quantity|l_extendedprice|l_discount|l_tax|l_returnflag|
// l_linestatus|l_shipdate|l_commitdate|l_receiptdate| l_shipinstruct|l_shipmode|l_comment|

object Sampler {
  def sample(lineitem: DataFrame, storageBudgetBytes: Long, e: Double, ci: Double): (List[RDD[_]], _) = {
    lineitem.show()

    val rows = lineitem.rdd

    val N = rows.count()

    //val ext_price_index = 5 // CLUSTER
    val ext_price_index = 9 // TEST

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

    //def check_sample(RDD: RDD[_]):

    //val lst : RDD[(Int,List[Double])] = rows.groupBy(x => (x(1), x(2))).map(x => (x._2.size, x._2.map(y => y(5).asInstanceOf[Int].toDouble).toList))
    //val vari = lst.map(x => (x._1, x._2.sum / x._2.length.toDouble, x._2)).map(x => (x._1, x._3.map(y => ((y - x._2) * (y - x._2)) / (x._1 - 1).toDouble ).sum))

    //vari.take(10).foreach(println)

    //mean(xs).flatMap(m => mean(xs.map(x => Math.pow(x-m, 2))))


    //rows.take(10).foreach(println)

    def sample_01(index_1 : Int, prb : Double): (RDD[_], Double) = {

      // Isolate each unique value and attribute a % that we want tot ake
      val fractions = rows
        .map(x => (x(index_1)))
        .distinct()
        .map( x => (x,prb))
        .collectAsMap()

      // Stratified sampling (using ...exact since we want all possible group
      val sample_data = rows
        .keyBy(x => (x(index_1)))
        .sampleByKeyExact(false, fractions)
        .map(x => x._2)

      //sample_data.groupBy(x => x(6)).take(10).foreach(println)

      // Calculate nh and sh2 according to formula in book about blinkDB
      val nh_sh2 = sample_data
        .groupBy(x => (x(index_1)))
        .map(x => ((x._1, x._2.size, x._2.map(y => y(ext_price_index).asInstanceOf[Int].toDouble).toList))) // y() is extendedprice -> use it to calculate variance
        .map(x => (x._1,x._2, x._3.sum / x._3.length.toDouble, x._3))
        .map(x => (x._1, x._2,x._4.map(y => ((y - x._3) * (y - x._3)) / (x._2).toDouble ).sum))

      // Compute Nh according to book about blinkDB
      val Nh = rows
        .groupBy(x => (x(index_1)))
        .map(x => ((x._1,x._2.size)))

      // Get all Nh , nh , sh value corresponding together
      val merged_rdd = nh_sh2
        .keyBy(x => x._1)
        .join(Nh.keyBy(x => x._1))
        .map(x => (x._2._1._2, x._2._1._3, x._2._2._2))

      // Compute error value per h
      val error_value = merged_rdd
        .map(x => (1 - x._1.toDouble / x._3.toDouble) * (x._3.toDouble / N.toDouble) * (x._3.toDouble / N.toDouble) * (x._2/ x._1.toDouble))

      // Collect and compute final error
      val true_error = z * sqrt(error_value.collect().sum)

      //sample_data.take(10).foreach(println)

      //nh_sh2.take(10).foreach(println)

      //println(z * sqrt(error_value.collect().sum))

      // Check if we are inbounds
      if (true_error <= e ){
        return (sample_data, true_error)
      }else{
        return (null, true_error)
      }

    }

    def sample_02(index_1 : Int, index_2 : Int, prb : Double): (RDD[_], Double) = {

      // Isolate each unique value and attribute a % that we want tot ake
      val fractions = rows
        .map(x => (x(index_1),x(index_2)))
        .distinct()
        .map( x => (x,prb))
        .collectAsMap()

      // Stratified sampling (using ...exact since we want all possible group
      val sample_data = rows
        .keyBy(x => (x(index_1),x(index_2)))
        .sampleByKeyExact(false, fractions)
        .map(x => x._2)

      //sample_data.groupBy(x => x(6)).take(10).foreach(println)

      // Calculate nh and sh2 according to formula in book about blinkDB
      val nh_sh2 = sample_data
        .groupBy(x => (x(index_1),x(index_2)))
        .map(x => ((x._1, x._2.size, x._2.map(y => y(ext_price_index).asInstanceOf[Int].toDouble).toList))) // y(5) is extendedprice -> use it to calculate variance
        .map(x => (x._1,x._2, x._3.sum / x._3.length.toDouble, x._3))
        .map(x => (x._1, x._2,x._4.map(y => ((y - x._3) * (y - x._3)) / (x._2).toDouble ).sum))

      // Compute Nh according to book about blinkDB
      val Nh = rows
        .groupBy(x => (x(index_1),x(index_2)))
        .map(x => ((x._1,x._2.size)))

      // Get all Nh , nh , sh value corresponding together
      val merged_rdd = nh_sh2
        .keyBy(x => x._1)
        .join(Nh.keyBy(x => x._1))
        .map(x => (x._2._1._2, x._2._1._3, x._2._2._2))

      // Compute error value per h
      val error_value = merged_rdd
        .map(x => (1 - x._1.toDouble / x._3.toDouble) * (x._3.toDouble / N.toDouble) * (x._3.toDouble / N.toDouble) * (x._2/ x._1.toDouble))

      // Collect and compute final error
      val true_error = z * sqrt(error_value.collect().sum)

      //sample_data.take(10).foreach(println)

      //nh_sh2.take(10).foreach(println)

      //println(z * sqrt(error_value.collect().sum))

      // Check if we are inbounds
      if (true_error <= e ){
        return (sample_data, true_error)
      }else{
        return (null, true_error)
      }

    }

    def sample_03(index_1 : Int, index_2 : Int, index_3 : Int ,prb : Double): (RDD[_], Double) = {

      // Isolate each unique value and attribute a % that we want tot ake
      val fractions = rows
        .map(x => (x(index_1),x(index_2),x(index_3)))
        .distinct()
        .map( x => (x,prb))
        .collectAsMap()

      // Stratified sampling (using ...exact since we want all possible group
      val sample_data = rows
        .keyBy(x => (x(index_1),x(index_2),x(index_3)))
        .sampleByKeyExact(false, fractions)
        .map(x => x._2)

      //sample_data.groupBy(x => x(6)).take(10).foreach(println)

      // Calculate nh and sh2 according to formula in book about blinkDB
      val nh_sh2 = sample_data
        .groupBy(x => (x(index_1),x(index_2),x(index_3)))
        .map(x => ((x._1, x._2.size, x._2.map(y => y(ext_price_index).asInstanceOf[Int].toDouble).toList))) // y(5) is extendedprice -> use it to calculate variance
        .map(x => (x._1,x._2, x._3.sum / x._3.length.toDouble, x._3))
        .map(x => (x._1, x._2,x._4.map(y => ((y - x._3) * (y - x._3)) / (x._2).toDouble ).sum))

      // Compute Nh according to book about blinkDB
      val Nh = rows
        .groupBy(x => (x(index_1),x(index_2),x(index_3)))
        .map(x => ((x._1,x._2.size)))

      // Get all Nh , nh , sh value corresponding together
      val merged_rdd = nh_sh2
        .keyBy(x => x._1)
        .join(Nh.keyBy(x => x._1))
        .map(x => (x._2._1._2, x._2._1._3, x._2._2._2))

      // Compute error value per h
      val error_value = merged_rdd
        .map(x => (1 - x._1.toDouble / x._3.toDouble) * (x._3.toDouble / N.toDouble) * (x._3.toDouble / N.toDouble) * (x._2/ x._1.toDouble))

      // Collect and compute final error
      val true_error = z * sqrt(error_value.collect().sum)

      //sample_data.take(10).foreach(println)

      //nh_sh2.take(10).foreach(println)

      //println(z * sqrt(error_value.collect().sum))

      // Check if we are inbounds
      if (true_error <= e ){
        return (sample_data, true_error)
      }else{
        return (null, true_error)
      }

    }

    def sample_04(index_1 : Int, index_2 : Int, index_3 : Int, index_4 : Int ,prb : Double): (RDD[_], Double) = {

      // Isolate each unique value and attribute a % that we want tot ake
      val fractions = rows
        .map(x => (x(index_1),x(index_2),x(index_3),x(index_4)))
        .distinct()
        .map( x => (x,prb))
        .collectAsMap()

      // Stratified sampling (using ...exact since we want all possible group
      val sample_data = rows
        .keyBy(x => (x(index_1),x(index_2),x(index_3),x(index_4)))
        .sampleByKeyExact(false, fractions)
        .map(x => x._2)

      //sample_data.groupBy(x => x(6)).take(10).foreach(println)

      // Calculate nh and sh2 according to formula in book about blinkDB
      val nh_sh2 = sample_data
        .groupBy(x => (x(index_1),x(index_2),x(index_3),x(index_4)))
        .map(x => ((x._1, x._2.size, x._2.map(y => y(ext_price_index).asInstanceOf[Int].toDouble).toList))) // y(5) is extendedprice -> use it to calculate variance
        .map(x => (x._1,x._2, x._3.sum / x._3.length.toDouble, x._3))
        .map(x => (x._1, x._2,x._4.map(y => ((y - x._3) * (y - x._3)) / (x._2).toDouble ).sum))

      // Compute Nh according to book about blinkDB
      val Nh = rows
        .groupBy(x => (x(index_1),x(index_2),x(index_3),x(index_4)))
        .map(x => ((x._1,x._2.size)))

      // Get all Nh , nh , sh value corresponding together
      val merged_rdd = nh_sh2
        .keyBy(x => x._1)
        .join(Nh.keyBy(x => x._1))
        .map(x => (x._2._1._2, x._2._1._3, x._2._2._2))

      // Compute error value per h
      val error_value = merged_rdd
        .map(x => (1 - x._1.toDouble / x._3.toDouble) * (x._3.toDouble / N.toDouble) * (x._3.toDouble / N.toDouble) * (x._2/ x._1.toDouble))

      // Collect and compute final error
      val true_error = z * sqrt(error_value.collect().sum)

      //sample_data.take(10).foreach(println)

      //nh_sh2.take(10).foreach(println)

      //println(z * sqrt(error_value.collect().sum))

      // Check if we are inbounds
      if (true_error <= e ){
        return (sample_data, true_error)
      }else{
        return (null, true_error)
      }

    }

    def sample(indexes : List[Int], start : Double = 0.03, stop : Double = 0.8, step : Double = 0.03): RDD[_] ={
      var ret : (RDD[_], Double) = (null,0)
      var prob : Double = start

      while(ret._1 == null && prob < stop) {
        indexes.length match {
          case 1 => ret = sample_01(indexes(0), prob)//._1
          case 2 => ret = sample_02(indexes(0), indexes(1),prob)//._1
          case 3 => ret = sample_03(indexes(0), indexes(1), indexes(2), prob)//._1
          case 4 => ret = sample_04(indexes(0), indexes(1), indexes(2), indexes(3), prob)//._1
          case _ => ret = null
        }
        println(prob)
        println(ret._2)

        prob = prob + step
      }
      return ret._1
    }


    // UPDATE IT IS USELESS TO SAMPLE ON ORDERKEY -> PRIMARY KEY => 100% of the table sampled everytime
    //  SAMPLE                                        ID CLuster
    // (quantity, shipdate)                             (4,10)
    // (shipdate, return_flag, linestatus)              (8,9,10)
    // (shipdate, discount, quantity)                   (10, 6 , 4 ) // TRY THIS BEFORE (4,10)
    // (shipmode, commitdate, shipdate, receipedate)    (14, 11, 10,12)
    // (quantity, shipmode, shipinstruct)               (4, 14, 13)
    // (suppkey, quantity)                              (2, 4)

    // Order on small :
    //lo_orderkey|lo_linenumber|lo_custkey|lo_partkey|lo_suppkey|lo_orderdate|lo_orderpriority|lo_shippriority|lo_quantity|lo_extendedprice|lo_ordertotalprice|lo_discount|lo_revenue|lo_supplycost|lo_tax|lo_commitdate|lo_shipmode|


    // Order on cluster :
    //|l_orderkey|l_partkey|l_suppkey|l_linenumber|l_quantity|l_extendedprice|l_discount|l_tax|l_returnflag|l_linestatus|l_shipdate|l_commitdate|l_receiptdate|l_shipinstruct|l_shipmode| l_comment|


    val list_samples_cluster : List[List[Int]] = List(List(4,6,10), List(8,9,10), List(10,11,12,14), List(4,13,14), List(2,4))
    val small_samples_cluster : List[List[Int]] = List(List(4,10), List(13,14), List(11,12)) // in case of very small budget
    val list_samples_test : List[List[Int]] = List(List(0,1), List(16), List(6,7))

    //def gen_return(arr : List[RDD[_]], arr_b : List[List[Int]])

    //val return_val = list_samples_cluster.map(x => (sample(x), x)).map(x => (x._1, getSize(x._1), x._2))
    val return_val = list_samples_test
      .map(x => (sample(x), x))
      .filter(x => x._1 != null)
      .map(x => (x._1, getSize(x._1), x._2))

    var size_tot : Long = 0
    var ret : (List[RDD[_]], List[List[Int]]) = (List(),List())

    for (elem <- return_val) {
      if (elem != null) {
        size_tot = size_tot + elem._2
        println("ELEM2")
        println(elem._2)
        if (size_tot <= storageBudgetBytes) {
          ret._1.+:(elem._1)
          ret._2.+:(elem._3)
        }else {
          size_tot = size_tot - elem._2
        }

      }
      println("Sizetot")
      println(size_tot)
    }

    if (ret._1.length == 0){
      val return_val = small_samples_cluster.map(x => (sample(x), x)).map(x => (x._1, getSize(x._1), x._2))

      size_tot = 0

      for (elem <- return_val) {
        if (elem != null) {
          size_tot = size_tot + elem._2
          if (size_tot <= storageBudgetBytes) {
            ret._1.+:(elem._1)
            ret._2.+:(elem._3)
          }
        }
      }
    }

    println("lliisstt")
    ret._2.foreach(println)

    /*val rr = sample_01(6, 0.001)
    rr._1.collect().foreach(println)
    println(rr._2)
    */
    //val ex_rdd = sampling(key_sample)

    //return (List(ex_rdd), key_sample)
    return ret
  }
}
