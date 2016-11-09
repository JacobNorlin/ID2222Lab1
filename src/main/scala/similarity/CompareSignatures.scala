package similarity

import org.apache.spark.rdd.RDD
import utils.{Context, Pipeline}

/**
  * Created by Jacob on 07-Nov-16.
  */
object CompareSignatures extends Pipeline[RDD[List[Int]], RDD[Double]] {

  def run(ctx: Context)(minHashMatrix: RDD[List[Int]]) = {
    val a = minHashMatrix.cartesian(minHashMatrix) map {
      case (m1, m2) => {
        compareSignatures(m1,m2)
      }
    }
    a
  }

  def compareSignatures(sigA: List[Int], sigB: List[Int]): Double = {
    val sameSigs: Double = (sigA intersect sigB).size
    val totalSigs: Double = sigA.size
    sameSigs/totalSigs
  }

}
