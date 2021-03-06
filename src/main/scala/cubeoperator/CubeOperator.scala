package cubeoperator

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

class CubeOperator(reducers: Int) {

  /*
 * This method gets as input one dataset, the grouping attributes of the cube (CUBE BY clause)
 * the attribute on which the aggregation is performed
 * and the aggregate function (it has to be one of "COUNT", "SUM", "MIN", "MAX", "AVG")
 * and returns an RDD with the result in the form of <key = string, value = double> pairs.
 * The key is used to uniquely identify a group that corresponds to a certain combination of attribute values.
 * You are free to do that following your own naming convention.
 * The value is the aggregation result.
 * You are not allowed to change the definition of this function or the names of the aggregate functions.
 * */
  def cube(dataset: Dataset, groupingAttributes: List[String], aggAttribute: String, agg: String): RDD[(String, Double)] = {

    val rdd = dataset.getRDD()
    val schema = dataset.getSchema()

    val rdd_partition = rdd.repartition(reducers)

    val index = groupingAttributes.map(x => schema.indexOf(x))
    val indexAgg = schema.indexOf(aggAttribute)
    val range = Range(0, index.length)
    val perms = Range(0, index.length).flatMap(i => range.combinations(i).toSet)
    
    agg match {
      case "COUNT" =>
        val single_lines = rdd
          .map(x => ((index.map(y => x.get(y))), 1))
          .groupBy(_._1)
          .mapValues(_.map(_._2).sum)

        val single_stringed = single_lines
          .map(x => (x._1.mkString(", ").replace("Some(", "").replace(")", ""), x._2))

        val partition_step_one = rdd_partition
          .mapPartitions( pa => pa.map(x => ((index.map(y => x.get(y))), 1)))
          .groupBy(_._1)
          .mapValues(_.map(_._2).sum)

        val partition_partial_upper = partition_step_one
          .mapPartitions( part => part
            .map(x => perms.map(p => (x._1.zipWithIndex.map{case(e, i) => if(p contains i) Some(e) else None}, x._2)))
            .flatMap(x => x))

        val partial_cubic = partition_partial_upper
          .groupBy(_._1)
          .mapValues(_.map(_._2).sum)

        val cubic = partial_cubic
          .repartition(1)
          .groupBy(_._1)
          .mapValues(_.map(_._2).sum)

        val ret = cubic
          .map(x => (x._1.mkString(", ").replace("Some(", "").replace(")", ""), x._2))
          .union(single_stringed)
          .map(x => (x._1, x._2.toDouble))

        return ret

      case "SUM" =>
        val single_lines = rdd
          .map(x => ((index.map(y => x.get(y))), x.get(indexAgg).asInstanceOf[Int]))
          .groupBy(_._1)
          .mapValues(_.map(_._2).sum)

        val single_stringed = single_lines
          .map(x => (x._1.mkString(", ").replace("Some(", "").replace(")", ""), x._2))

        val partition_step_one = rdd_partition
          .mapPartitions( pa => pa.map(x => ((index.map(y => x.get(y))), x.get(indexAgg).asInstanceOf[Int])))
          .groupBy(_._1)
          .mapValues(_.map(_._2).sum)

        val partition_partial_upper = partition_step_one
          .mapPartitions( part => part.map(x => perms
            .map(p => (x._1.zipWithIndex.map{case(e, i) => if(p contains i) Some(e) else None}, x._2)))
            .flatMap(x => x))

        val partial_cubic = partition_partial_upper
          .groupBy(_._1)
          .mapValues(_.map(_._2).sum)

        val cubic = partial_cubic
          .repartition(1)
          .groupBy(_._1)
          .mapValues(_.map(_._2).sum)

        val ret = cubic
          .map(x => (x._1.mkString(", ").replace("Some(", "").replace(")", ""), x._2))
          .union(single_stringed)
          .map(x => (x._1, x._2.toDouble))

        return ret

      case "MIN" =>
        val single_lines = rdd
          .map(x => ((index.map(y => x.get(y))), x.get(indexAgg).asInstanceOf[Int]))
          .groupBy(_._1)
          .mapValues(_.minBy(_._2))
          .map(x => x._2)

        val single_stringed = single_lines
          .map(x => (x._1.mkString(", ").replace("Some(", "").replace(")", ""), x._2))

        val partition_step_one = rdd_partition
          .mapPartitions( pa => pa.map(x => ((index.map(y => x.get(y))), x.get(indexAgg).asInstanceOf[Int])))
          .groupBy(_._1)
          .mapValues(_.minBy(_._2))
          .map(x => x._2)

        val partition_partial_upper = partition_step_one
          .mapPartitions( part => part
            .map(x => perms.map(p => (x._1.zipWithIndex.map{case(e, i) => if(p contains i) Some(e) else None}, x._2)))
            .flatMap(x => x))

        val partial_cubic = partition_partial_upper
          .groupBy(_._1)
          .mapValues(_.minBy(_._2))
          .map(x => x._2)

        val cubic = partial_cubic
          .repartition(1)
          .groupBy(_._1)
          .mapValues(_.minBy(_._2))
          .map(x => x._2)

        val ret = cubic
          .map(x => (x._1.mkString(", ").replace("Some(", "").replace(")", ""), x._2))
          .union(single_stringed)
          .map(x => (x._1, x._2.toDouble))

        return ret

      case "MAX" =>
        val single_lines = rdd
          .map(x => ((index.map(y => x.get(y))), x.get(indexAgg).asInstanceOf[Int]))
          .groupBy(_._1)
          .mapValues(_.maxBy(_._2))
          .map(x => x._2)

        val single_stringed = single_lines
          .map(x => (x._1.mkString(", ").replace("Some(", "").replace(")", ""), x._2))

        val partition_step_one = rdd_partition
          .mapPartitions( pa => pa
            .map(x => ((index.map(y => x.get(y))), x.get(indexAgg).asInstanceOf[Int])))
          .groupBy(_._1)
          .mapValues(_.maxBy(_._2))
          .map(x => x._2)

        val partition_partial_upper = partition_step_one
          .mapPartitions( part => part
            .map(x => perms.map(p => (x._1.zipWithIndex.map{case(e, i) => if(p contains i) Some(e) else None}, x._2)))
            .flatMap(x => x))

        val partial_cubic = partition_partial_upper
          .groupBy(_._1)
          .mapValues(_.maxBy(_._2))
          .map(x => x._2)

        val cubic = partial_cubic
          .repartition(1)
          .groupBy(_._1)
          .mapValues(_.maxBy(_._2))
          .map(x => x._2)

        val ret = cubic
          .map(x => (x._1.mkString(", ").replace("Some(", "").replace(")", ""), x._2))
          .union(single_stringed).map(x => (x._1, x._2.toDouble))

        return ret

      case "AVG" =>
        val count_rdd = cube(dataset, groupingAttributes, aggAttribute, "COUNT")

        val sum_rdd = cube(dataset, groupingAttributes, aggAttribute, "SUM")

        return count_rdd
          .join(sum_rdd)
          .map(x => (x._1, x._2._2 / x._2._1) )

      case _ =>
        println("BAD AGGREGATE")

        return null // Return null if bad aggregate
    }

    /*
    THIS MULTILINE COMMENT HOLDS THE FIRST WORKING VERSION OF THE CODE BUT HAD NO PARTITIONING

      val step_one_map = rdd.map(x => ((index.map(y => x.get(y))), x.get(indexAgg).asInstanceOf[Int])).groupBy(_._1).mapValues(_.map(_._2).sum)
      val partial_upper = step_one_map.map(x => perms.map(p => (x._1.zipWithIndex.map{case(e, i) => if(p contains i) Some(e) else None}, x._2))).flatMap(x => x)
      val cubic = partial_upper.groupBy(_._1).mapValues(_.map(_._2).sum)
      cubic.map(x => (x._1.mkString(", ").replace("Some(", "").replace(")", ""), x._2)).filter(x => x._2 == 61867).take(100).foreach(println)
      return cubic.map(x => (x._1.mkString(", ").replace("Some(", "").replace(")", ""), x._2))

     */

  }

  def cube_naive(dataset: Dataset, groupingAttributes: List[String], aggAttribute: String, agg: String): RDD[(String, Double)] = {

    val rdd = dataset.getRDD()
    val schema = dataset.getSchema()

    val index = groupingAttributes.map(x => schema.indexOf(x))
    val indexAgg = schema.indexOf(aggAttribute)
    val range = Range(0, index.length)
    val perms = Range(0, index.length).flatMap(i => range.combinations(i).toSet)

    val partition_step_one = rdd
      .map(x => ((index.map(y => x.get(y))), x.get(indexAgg).asInstanceOf[Int]))

    val single_lines = rdd
      .map(x => ((index.map(y => x.get(y))), x.get(indexAgg).asInstanceOf[Int]))

    val map01 = partition_step_one
        .map(x => perms.map(p => (x._1.zipWithIndex.map{case(e, i) => if(p contains i) Some(e) else None}, x._2)))
        .flatMap(x => x)

    agg match {
      case "COUNT" =>
        val map_reduce = map01
          .map(x => (x._1, 1))
          .reduceByKey(_+_)
          .map(x => (x._1.mkString(", ").replace("Some(", "").replace(")", ""), x._2.toDouble))

        val single_reduce = single_lines
          .map(x => (x._1, 1))
          .reduceByKey(_+_)
          .map(x => (x._1.mkString(", ").replace("Some(", "").replace(")", ""), x._2.toDouble))

        return map_reduce.union(single_reduce)

      case "SUM" =>
        val map_reduce = map01
          .reduceByKey(_+_)
          .map(x => (x._1.mkString(", ").replace("Some(", "").replace(")", ""), x._2.toDouble))

        val single_reduce = single_lines
          .reduceByKey(_+_)
          .map(x => (x._1.mkString(", ").replace("Some(", "").replace(")", ""), x._2.toDouble))

        return map_reduce.union(single_reduce)

      case "MIN" =>
        val map_reduce = map01
          .reduceByKey(math.min(_,_))
          .map(x => (x._1.mkString(", ").replace("Some(", "").replace(")", ""), x._2.toDouble))

        val single_reduce = single_lines
          .reduceByKey(math.min(_,_))
          .map(x => (x._1.mkString(", ").replace("Some(", "").replace(")", ""), x._2.toDouble))

        return map_reduce.union(single_reduce)

      case "MAX" =>
        val map_reduce = map01
          .reduceByKey(math.max(_,_))
          .map(x => (x._1.mkString(", ").replace("Some(", "").replace(")", ""), x._2.toDouble))

        val single_reduce = single_lines
          .reduceByKey(math.max(_,_))
          .map(x => (x._1.mkString(", ").replace("Some(", "").replace(")", ""), x._2.toDouble))

        return map_reduce.union(single_reduce)

      case "AVG" =>
        val count = cube_naive(dataset, groupingAttributes, aggAttribute, "COUNT")
        val sum = cube_naive(dataset, groupingAttributes, aggAttribute, "SUM")

        return count
          .join(sum)
          .map(x => (x._1, x._2._2 / x._2._1) )

      case _ =>
        println("BAD AGGREGATE")
        return null
    }
  }

}
