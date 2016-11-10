package similarity

import org.apache.spark.rdd.RDD
import utils._

import scala.collection.immutable.IntMap

/**
  * Created by Jacob on 07-Nov-16.
  */
object Shingling extends Pipeline[RDD[String], (RDD[Set[Int]], Map[Int, Int])] {

  def run(ctx: Context)(posts: RDD[String]): (RDD[Set[Int]], Map[Int, Int]) = {
    val shingleSize = ctx.shingleSize
    val hashes: RDD[Set[Int]] = posts.map(post => shingleSet(post, shingleSize))
    val map = shingleRowMap(hashes)
    println("Shingling done")
    (hashes, map)
  }

  /**
    * Generates a map of the form (shingle -> rownumber)
    * @param hashedShingles
    * @return
    */
  def shingleRowMap(hashedShingles: RDD[Set[Int]]): Map[Int, Int] = {
    val allShingles = hashedShingles.reduce(_ union _)
    allShingles.zipWithIndex.toMap[Int, Int]
  }

  /**
    * Converts a string to a set of shingles of length k
    *
    * @param text
    * @param k
    * @return
    */
  def shingleSet(text: String, k: Int): Set[Int] = {
    text.zipWithIndex.map {
      case (c, i) => hashShingle(kShingle(text, k, i))
    } toSet
  }

  /**
    * Generates a shingle of length k from i to k for a given string
    *
    * @param text
    * @param k
    * @param index
    * @return
    */
  def kShingle(text: String, k: Int, index: Int): String = {
    text.substring(index, Math.min(index + k, text.length))
  }

  def hashShingle(shingle: String): Int = {
    shingle.hashCode
  }

}
