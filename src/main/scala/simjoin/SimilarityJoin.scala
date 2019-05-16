package simjoin

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row

import org.slf4j.Logger
import org.slf4j.LoggerFactory

class SimilarityJoin(numAnchors: Int, distThreshold:Int) extends java.io.Serializable {
  val logger = LoggerFactory.getLogger("SimilarityJoin")
  var rdd: RDD[String] = null
  
  /*
   * this method gets as input a dataset and the index of an attribute
   * of the dataset, and returns the result of the similarity self join on
   * that attribute.
   * */  
  def similarity_join(dataset: Dataset, attrIndex: Int) : RDD[(String, String)] = {
    
    val data = dataset.getRDD()
    val fraction = numAnchors.toDouble / data.count().toDouble

    val anchors = data.sample(false, fraction)
    val anchors_val : RDD[(String, Long)] = anchors.map(x => x.get(attrIndex).asInstanceOf[String]).zipWithIndex()
    val true_anchors_nb = anchors.count()

    // Cut-off / edit distance -> Levenshtein
    def distance(s1: String, s2: String): Int = {
      val dist = Array.tabulate(s2.length + 1, s1.length + 1) { (j, i) => if (j == 0) i else if (i == 0) j else 0 }

      @inline
      def minimum(i: Int*): Int = i.min

      for {j <- dist.indices.tail
           i <- dist(0).indices.tail} dist(j)(i) =
        if (s2(j - 1) == s1(i - 1)) dist(j - 1)(i - 1)
        else minimum(dist(j - 1)(i) + 1, dist(j)(i - 1) + 1, dist(j - 1)(i - 1) + 1)

      dist(s2.length)(s1.length)
    }

    def outer_val(dist_to_closest_anchor : Int, dist_to_anchor : Int , d_threshold : Int): Boolean ={
      return dist_to_anchor <= dist_to_closest_anchor + 2 * d_threshold
    }

    // Spark does not allow nested map with RDDs so ....
    val anchor_cartesian = data.cartesian(anchors_val)

    // Compute dit dist for all possiple pairings with anchors
    val anchors_dist = anchor_cartesian.map(x=> (x._1, x._2, distance(x._1.get(attrIndex).asInstanceOf[String], x._2._1)) )

    // Compute list of distance for future outer part computation
    val distances_list = anchors_dist
      .groupBy(x => x._1)
      .mapValues(_.map(y => y._3))

    // Keep only index with lowest distance
    val low_index = anchors_dist
      .groupBy(x=> x._1)
      .mapValues(_.minBy(y => y._3))
      .map(z => (z._1, z._2._2._2, z._2._3))

    // RDD to the format : Row, AnchorIndex, Dist_to_closest_anchor, List(Belongs to outerpartition of ith anchor)
    val outer_belongings = low_index
      .zip(distances_list)
      .map(x => (x._1._1, x._1._2, x._1._3, x._2._2.map(y => outer_val(x._1._3, y,distThreshold))))

    val range_rdd = List.range(0, true_anchors_nb)

    // Filtered by partition , oputer partition
    val list_fitered:List[RDD[String]]= for (i <- range_rdd) yield outer_belongings
      .filter(x => x._4.toList(i.toInt))
      .map(x => x._1.getAs[String](attrIndex))

    val cross_filtered = list_fitered.map(x=> x.cartesian(x)) // match each element with every other element in its partition

    // remove unitary comparaision and sort tuples alphabetically for easy duplicates removing
    val cross_sorted = cross_filtered
      .map(partion => partion.filter(x => !(x._1 == x._2)).map(x => if(x._1 > x._2) x.swap else x) )

    // merge all partition and keep each pair only once
    val union_job = cross_sorted.reduce(_ union _).distinct()

    // Return only pairs smaller or equal to treshold EACH PAIR is evaluated ONCE
    // The only duplicate distance evaluation is the one with anchor itself
    val final_result = union_job.filter(x => distance(x._1,x._2) <= distThreshold)

    // We need to output an RDD with (a,b) as well as (b,a) since we sorted we have to gen swapped values too
    val final_result_swap = final_result.map(x => (x._2, x._1))

    return final_result.union(final_result_swap)
  }
}

