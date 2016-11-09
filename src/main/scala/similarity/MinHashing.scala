package similarity

import org.apache.spark.rdd.RDD
import utils.{Context, Pipeline}

import scala.util.Random



/**
  * Created by Jacob on 07-Nov-16.
  */
object MinHashing extends Pipeline[(RDD[Set[Int]], Map[Int, Int]), RDD[List[Int]]] {

  val p = 2147483647

  def genCoefficients(n: Int) = {
    val rand = new Random(System.currentTimeMillis())
    (0 to n) map (_ => (rand.nextInt(p), rand.nextInt(p)))
  }

  def genHashFunctions(n: Int): List[(Int => Int)] = {
    genCoefficients(n) map {
      case (a, b) => ((x: Int) => (a * x + b) % p)
    } toList
  }

  def run(ctx: Context)(t: (RDD[Set[Int]], Map[Int, Int])) = {
    val hashFunctions: List[(Int => Int)] = genHashFunctions(25)
    val shinglesSignatures = t._1
    val shingleMap = t._2
    shinglesSignatures map (s => minHash(s, shingleMap, hashFunctions))
  }

  def minHash(shingleSignatures: Set[Int], shingleMap: Map[Int, Int], hashFunctions: List[Int => Int]): List[Int] = {
    hashFunctions.map((h: (Int => Int)) => {
      shingleSignatures.map((s: Int) => {
        val rowNum = shingleMap.getOrElse[Int](s, -1)
        h(rowNum)
      }).min
    })
  }


}
