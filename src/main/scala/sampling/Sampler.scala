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

    //val random_sample = rows.sample(false, 0.1)
    //val estimate_row_size : Long = SizeEstimator.estimate(random_sample.collect()) / random_sample.count()
    val estimate_row_size : Long = 836

    val ext_price_index = 5 // CLUSTER
    //val ext_price_index = 13 // TEST -> supplycost

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
      return input.count() * estimate_row_size
    }
    def emergency_sample() : (List[RDD[_]],_) ={
      var frac = 0.30

      var ret = rows.sample(false, frac)

      while (getSize(ret) > storageBudgetBytes && frac > 0){
        frac = frac - 0.05
        ret = rows.sample(false, frac)
      }
      if (frac < 0){
        return (List(rows.sample(false, 0.01)), List(1,2,3,4,5,6,7,8,9))
      }else {
        return (List(ret), List(List(1, 2, 3, 4, 5, 6, 7, 8, 9)))
      }
    }

    val z = getZ()

    //def check_sample(RDD: RDD[_]):

    //val lst : RDD[(Int,List[Double])] = rows.groupBy(x => (x(1), x(2))).map(x => (x._2.size, x._2.map(y => y(5).asInstanceOf[Int].toDouble).toList))
    //val vari = lst.map(x => (x._1, x._2.sum / x._2.length.toDouble, x._2)).map(x => (x._1, x._3.map(y => ((y - x._2) * (y - x._2)) / (x._1 - 1).toDouble ).sum))

    //vari.take(10).foreach(println)

    //mean(xs).flatMap(m => mean(xs.map(x => Math.pow(x-m, 2))))


    //rows.take(10).foreach(println)

    def sample_01(index_1 : Int, prb : Double, Nh_Sh :  RDD[((Any), Int, Double)]): (RDD[_], Double) = {

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

      // Calculate nh and sh2 according to formula in book about blinkDB
      val nh_sh2 = sample_data
        .groupBy(x => (x(index_1)))
        .map(x => (x._1, x._2.size))


      // Get all Nh , nh , sh value corresponding together
      val merged_rdd = nh_sh2
        .keyBy(x => x._1)
        .join(Nh_Sh.keyBy(x => x._1))
        .map(x => (x._2._1._2, x._2._2._2, x._2._2._3)) // (nh, Nh, Sh)

      //val n:Double = nh_sh2.map(x => x._2).collect().sum // sum of nh = n
      val n = N

      // Compute error value per h
      val error_value = merged_rdd
        .map(x => ((n / x._1) * Math.pow(x._2, 2) * x._3 )) // n/nh * Nh² * Sh²  for all h
        .collect()
        .sum / n // sum over all h and divide by n this is equal to V/N


      // Collect and compute final error
      val true_error = z * Math.pow(error_value, 0.5) // WE WANT THIS TO BE WHITHIN e% of the mean

      val mean_sample : List[Double] = sample_data
        .map(x => x(ext_price_index))
        .collect()
        .map(x => x.asInstanceOf[java.math.BigDecimal].doubleValue())
        .toList

      val mean_value : Double = mean_sample.foldLeft(0.0) (_ + _) /// mean_sample.length

      val perc : Double = true_error / mean_value

      // Check if we are inbounds
      if (perc <= e && perc > 0 ) {
        return (sample_data, perc)
      }else if (perc == 0 && prb > 0.3){ // If it matches only to one elem drop said sample
        return (null, perc)
      }else{
        return (null, perc)
      }

    }

    def sample_02(index_1 : Int, index_2 : Int, prb : Double, Nh_Sh :  RDD[((Any,Any), Int, Double)]): (RDD[_], Double) = {

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

      // Calculate nh and sh2 according to formula in book about blinkDB
      val nh_sh2 = sample_data
        .groupBy(x => (x(index_1),x(index_2)))
        .map(x => (x._1, x._2.size))


      // Get all Nh , nh , sh value corresponding together
      val merged_rdd = nh_sh2
        .keyBy(x => x._1)
        .join(Nh_Sh.keyBy(x => x._1))
        .map(x => (x._2._1._2, x._2._2._2, x._2._2._3)) // (nh, Nh, Sh)

      //val n:Double = nh_sh2.map(x => x._2).collect().sum // sum of nh = n
      val n = N

      // Compute error value per h
      val error_value = merged_rdd
        .map(x => ((n / x._1) * Math.pow(x._2, 2) * x._3 )) // n/nh * Nh² * Sh²  for all h
        .collect()
        .sum / n // sum over all h and divide by n this is equal to V/N


      // Collect and compute final error
      val true_error = z * Math.pow(error_value, 0.5) // WE WANT THIS TO BE WHITHIN e% of the mean

      val mean_sample : List[Double] = sample_data
        .map(x => x(ext_price_index))
        .collect()
        .map(x => x.asInstanceOf[java.math.BigDecimal].doubleValue())
        .toList

      val mean_value : Double = mean_sample.foldLeft(0.0) (_ + _) /// mean_sample.length

      val perc : Double = true_error / mean_value

      // Check if we are inbounds
      if (perc <= e && perc > 0 ) {
        return (sample_data, perc)
      }else if (perc == 0 && prb > 0.3){ // If it matches only to one elem drop said sample
        return (null, perc)
      }else{
        return (null, perc)
      }

    }

    def sample_03(index_1 : Int, index_2 : Int, index_3 : Int, prb : Double, Nh_Sh :  RDD[((Any,Any,Any), Int, Double)]): (RDD[_], Double) = {

      // Isolate each unique value and attribute a % that we want tot ake
      val fractions = rows
        .map(x => (x(index_1),x(index_2), x(index_3)))
        .distinct()
        .map( x => (x,prb))
        .collectAsMap()

      // Stratified sampling (using ...exact since we want all possible group
      val sample_data = rows
        .keyBy(x => (x(index_1),x(index_2), x(index_3)))
        .sampleByKeyExact(false, fractions)
        .map(x => x._2)

      // Calculate nh and sh2 according to formula in book about blinkDB
      val nh_sh2 = sample_data
        .groupBy(x => (x(index_1),x(index_2), x(index_3)))
        .map(x => (x._1, x._2.size))


      // Get all Nh , nh , sh value corresponding together
      val merged_rdd = nh_sh2
        .keyBy(x => x._1)
        .join(Nh_Sh.keyBy(x => x._1))
        .map(x => (x._2._1._2, x._2._2._2, x._2._2._3)) // (nh, Nh, Sh)

      //val n:Double = nh_sh2.map(x => x._2).collect().sum // sum of nh = n
      val n = N

      // Compute error value per h
      val error_value = merged_rdd
        .map(x => ((n / x._1) * Math.pow(x._2, 2) * x._3 )) // n/nh * Nh² * Sh²  for all h
        .collect()
        .sum / n // sum over all h and divide by n this is equal to V/N


      // Collect and compute final error
      val true_error = z * Math.pow(error_value, 0.5) // WE WANT THIS TO BE WHITHIN e% of the mean

      val mean_sample : List[Double] = sample_data
        .map(x => x(ext_price_index))
        .collect()
        .map(x => x.asInstanceOf[java.math.BigDecimal].doubleValue())
        .toList

      val mean_value : Double = mean_sample.foldLeft(0.0) (_ + _) /// mean_sample.length

      val perc : Double = true_error / mean_value

      // Check if we are inbounds
      if (perc <= e && perc > 0 ) {
        return (sample_data, perc)
      }else if (perc == 0 && prb > 0.3){ // If it matches only to one elem drop said sample
        return (null, perc)
      }else{
        return (null, perc)
      }

    }

    // x(index_1),x(index_2),x(index_3),x(index_4)

    def sample_04(index_1 : Int, index_2 : Int, index_3 : Int, index_4 : Int ,prb : Double, Nh_Sh :  RDD[((Any,Any,Any,Any), Int, Double)]): (RDD[_], Double) = {

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

      // Calculate nh and sh2 according to formula in book about blinkDB
      val nh_sh2 = sample_data
        .groupBy(x => (x(index_1),x(index_2),x(index_3),x(index_4)))
        .map(x => (x._1, x._2.size))


      // Get all Nh , nh , sh value corresponding together
      val merged_rdd = nh_sh2
        .keyBy(x => x._1)
        .join(Nh_Sh.keyBy(x => x._1))
        .map(x => (x._2._1._2, x._2._2._2, x._2._2._3)) // (nh, Nh, Sh)

      //val n:Double = nh_sh2.map(x => x._2).collect().sum // sum of nh = n
      val n = N

      // Compute error value per h
      val error_value = merged_rdd
        .map(x => ((n / x._1) * Math.pow(x._2, 2) * x._3 )) // n/nh * Nh² * Sh²  for all h
        .collect()
        .sum / n // sum over all h and divide by n this is equal to V/N


      // Collect and compute final error
      val true_error = z * Math.pow(error_value, 0.5) // WE WANT THIS TO BE WHITHIN e% of the mean

      val mean_sample : List[Double] = sample_data
        .map(x => x(ext_price_index))
        .collect()
        .map(x => x.asInstanceOf[java.math.BigDecimal].doubleValue())
        .toList

      val mean_value : Double = mean_sample.foldLeft(0.0) (_ + _) /// mean_sample.length

      val perc : Double = true_error / mean_value

      // Check if we are inbounds
      if (perc <= e && perc > 0 ) {
        return (sample_data, perc)
      }else if (perc == 0 && prb > 0.3){ // If it matches only to one elem drop said sample
        return (null, perc)
      }else{
        return (null, perc)
      }

    }

    def sample(indexes : List[Int], Nh_Sh : RDD[_] ,start : Double = 0.03, stop : Double = 0.8, step : Double = 0.03): RDD[_] ={
      var ret : (RDD[_], Double) = (null,0)
      var prob : Double = start

      while(ret._1 == null && prob < stop) {
        indexes.length match {
          case 1 => ret = sample_01(indexes(0), prob, Nh_Sh.asInstanceOf[RDD[((Any), Int, Double)]])//._1
          case 2 => ret = sample_02(indexes(0), indexes(1),prob, Nh_Sh.asInstanceOf[RDD[((Any,Any), Int, Double)]])//._1
          case 3 => ret = sample_03(indexes(0), indexes(1), indexes(2), prob, Nh_Sh.asInstanceOf[RDD[((Any,Any,Any), Int, Double)]])//._1
          case 4 => ret = sample_04(indexes(0), indexes(1), indexes(2), indexes(3), prob, Nh_Sh.asInstanceOf[RDD[((Any,Any,Any,Any), Int, Double)]])//._1
          case _ => ret = null
        }

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


    try {

      val list_samples_cluster : List[List[Int]] = List(List(4,6,10), List(8,9,10), List(10,11,12,14), List(4,13,14), List(2,4))
    //val list_samples_cluster : List[List[Int]] = List(List(4,6,10))
      val small_samples_cluster : List[List[Int]] = List(List(4,10), List(13,14), List(11,12)) // in case of very small budget
      val list_samples_test : List[List[Int]] = List(List(4,6), List(16), List(6,7))

      lazy val Nh_Sh_1 = rows
        .groupBy(x => (x(4),x(6),x(10)))
        .map(x => ((x._1,x._2.size, x._2.map(y => y(ext_price_index).asInstanceOf[java.math.BigDecimal].doubleValue()).toList)))
        .map(x => (x._1,x._2, x._3.sum / x._3.length.toDouble, x._3))
        .map(x => (x._1, x._2,x._4.map(y => Math.pow(y - x._3, 2) / (x._2).toDouble ).sum)) // key, Nh, Sh

      lazy val Nh_Sh_2 = rows
        .groupBy(x => (x(8),x(9),x(10)))
        .map(x => ((x._1,x._2.size, x._2.map(y => y(ext_price_index).asInstanceOf[java.math.BigDecimal].doubleValue()).toList)))
        .map(x => (x._1,x._2, x._3.sum / x._3.length.toDouble, x._3))
        .map(x => (x._1, x._2,x._4.map(y => Math.pow(y - x._3, 2) / (x._2).toDouble ).sum)) // key, Nh, Sh

      lazy val Nh_Sh_3 = rows
        .groupBy(x => (x(10),x(11),x(12),x(14)))
        .map(x => ((x._1,x._2.size, x._2.map(y => y(ext_price_index).asInstanceOf[java.math.BigDecimal].doubleValue()).toList)))
        .map(x => (x._1,x._2, x._3.sum / x._3.length.toDouble, x._3))
        .map(x => (x._1, x._2,x._4.map(y => Math.pow(y - x._3, 2) / (x._2).toDouble ).sum)) // key, Nh, Sh

    lazy val Nh_Sh_4 = rows
      .groupBy(x => (x(4),x(13),x(14)))
      .map(x => ((x._1,x._2.size, x._2.map(y => y(ext_price_index).asInstanceOf[java.math.BigDecimal].doubleValue()).toList)))
      .map(x => (x._1,x._2, x._3.sum / x._3.length.toDouble, x._3))
      .map(x => (x._1, x._2,x._4.map(y => Math.pow(y - x._3, 2) / (x._2).toDouble ).sum)) // key, Nh, Sh

    lazy val Nh_Sh_5 = rows
      .groupBy(x => (x(2),x(4)))
      .map(x => ((x._1,x._2.size, x._2.map(y => y(ext_price_index).asInstanceOf[java.math.BigDecimal].doubleValue()).toList)))
      .map(x => (x._1,x._2, x._3.sum / x._3.length.toDouble, x._3))
      .map(x => (x._1, x._2,x._4.map(y => Math.pow(y - x._3, 2) / (x._2).toDouble ).sum)) // key, Nh, Sh

    val Nh_Sh_lst = List(Nh_Sh_1, Nh_Sh_2, Nh_Sh_3, Nh_Sh_4, Nh_Sh_5)

    //val Nh_Sh_lst = List(Nh_Sh_1)

    //def gen_return(arr : List[RDD[_]], arr_b : List[List[Int]])


      var return_val = list_samples_cluster
        .zip(Nh_Sh_lst)
        .map(x => (sample(x._1, x._2, 0.15, 0.8, 0.1), x._1))
        .map(x => (x._1, getSize(x._1), x._2))

      //var return_val = list_samples_test
      //  .map(x => (sample(x), x))
      //  .filter(x => x._1 != null)
      //  .map(x => (x._1, getSize(x._1), x._2))

      var size_tot: Long = 0
      var i: Int = 0

      while (i < return_val.length) {
        size_tot = size_tot + return_val(i)._2

        if (size_tot > storageBudgetBytes) { // remove additional size and drop elem
          size_tot = size_tot - return_val(i)._2
          return_val = return_val.filter(x => x._3 != return_val(i)._3)
        } else {
          i = i + 1
        }
      }


      val rdds = return_val.map(x => x._1).toList
      val free_obj = return_val.map(x => x._3).toList


      if (rdds.length == 0){
        println("Sampling failed go to fallback")
        return emergency_sample()
      }else {
        return (rdds, free_obj)
      }

    } catch{
      case _ : Throwable =>
        println("Sampling failed go to fallback")
        return  emergency_sample()
    }

    //return return_val.map(x => (x._1, x._3))

    /*val rr = sample_01(6, 0.001)
    rr._1.collect().foreach(println)
    println(rr._2)
    */
    //val ex_rdd = sampling(key_sample)

    //return (List(ex_rdd), key_sample)
  }
}
