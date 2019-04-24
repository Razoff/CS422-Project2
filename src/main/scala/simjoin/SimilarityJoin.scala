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

    // Return index of closest anchor
    // Useless now delete I guess
    def pick_anchor(elem: String, anchors: RDD[(String, Long)]) : Long = {
      val returns : RDD[(Int, Long)] = anchors.map(x => (distance(elem, x._1), x._2))

      returns.collect().foreach(println)

      val ret = returns.reduce((x,y) => if(x._1 < y._1) x else y)

      return ret._2
    }

    // Spark does not allow nested map with RDDs so ....
    val clo = data.cartesian(anchors_val)

    // Compute dit dist for all possiple pairings
    val cloclo = clo.map(x=> (x._1, x._2, distance(x._1.get(attrIndex).asInstanceOf[String], x._2._1)) )

    // Keep only index with lowest distance
    val nex = cloclo.groupBy(x=> x._1).mapValues(_.minBy(y => y._3)).map(z => (z._1, z._2._2._2, z._2._3))

    // Each row is matched with its cluster form is (Row, index, dist_to_index)
    nex.take(10).foreach(println)

    return null;
  }
}

