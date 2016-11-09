package similarity

import org.apache.spark.rdd.RDD
import utils.{Context, Pipeline}

/**
  * Created by Jacob on 07-Nov-16.
  */
object CompareSets extends Pipeline[(RDD[Set[Int]], Map[Int, Int]), RDD[Double]] {

  def run(ctx: Context)(shingleSignatures: (RDD[Set[Int]], Map[Int, Int])) = {
    val a = shingleSignatures._1.cartesian(shingleSignatures._1) map {
      case (m1, m2) => {
        jaccardSimilarity(m1,m2)
      }
    }
    a
  }
  def jaccardSimilarity(a: Set[Int], b: Set[Int]): Double = {
    val intersection: Double = (a intersect b).size
    val union: Double = (a union b).size
    intersection / union
  }
}
