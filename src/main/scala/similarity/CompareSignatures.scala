package similarity

import org.apache.spark.rdd.RDD
import utils.{Context, Pipeline}

/**
  * Created by Jacob on 07-Nov-16.
  */
object CompareSignatures extends Pipeline[RDD[List[Int]], RDD[List[Int]]] {

  def run(ctx: Context)(minHashMatrix: RDD[List[Int]]) = {
    minHashMatrix.cartesian(minHashMatrix) foreach {
      case(m1,m2) => {
        val comp = compareSignatures(m1,m2)
        if (comp > 0) println(comp)
      }
    }
    minHashMatrix
  }

  def compareSignatures(sigA: List[Int], sigB: List[Int]): Double = {
    val sameSigs: Double = (sigA intersect sigB).size
    val totalSigs: Double = sigA.size
    sameSigs/totalSigs
  }

}
