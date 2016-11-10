package similarity

import org.apache.spark.rdd.RDD
import utils.{Context, Pipeline}

import scala.util.Random



/**
  * Created by Jacob on 07-Nov-16.
  */
object MinHashing extends Pipeline[(RDD[Set[Int]], Map[Int, Int]), RDD[List[Int]]] {

  val p = 2147483647

  /**
    * Generates n tuples of random numbers smaller than p
    * @param n
    * @return
    */
  private def genCoefficients(n: Int) = {
    val rand = new Random(System.currentTimeMillis())
    (0 to n) map (_ => (rand.nextInt(p), rand.nextInt(p)))
  }

  /**
    * Generates n random hash functions of the form (ax+b)%p
    * @param n
    * @return
    */
  def genHashFunctions(n: Int): List[(Int => Int)] = {
    genCoefficients(n) map {
      case (a, b) => ((x: Int) => (a * x + b) % p)
    } toList
  }

  def run(ctx: Context)(t: (RDD[Set[Int]], Map[Int, Int])) = {
    val numHashFunctions = ctx.numHashFunctions
    val hashFunctions: List[(Int => Int)] = genHashFunctions(numHashFunctions)
    val shinglesSignatures = t._1
    val shingleMap = t._2
    shinglesSignatures map (s => minHash(s, shingleMap, hashFunctions))
  }

  /**
    * Generates the minhash signature of a set of shingleSignatures.
    * Takes a set of random hash functions and generates a list of
    * the smallest value for each hash function over the set of shingles signatures.
    * @param shingleSignatures
    * @param shingleMap
    * @param hashFunctions
    * @return
    */
  def minHash(shingleSignatures: Set[Int], shingleMap: Map[Int, Int], hashFunctions: List[Int => Int]): List[Int] = {
    hashFunctions.map((h: (Int => Int)) => {
      shingleSignatures.map((s: Int) => {
        val rowNum = shingleMap(s)
        h(rowNum)
      }).min
    })
  }


}
