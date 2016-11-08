package similarity

import org.apache.spark.rdd.RDD
import utils._
/**
  * Created by Jacob on 07-Nov-16.
  */
object Shingling extends Pipeline[RDD[String], RDD[Set[Int]]]{
  def run(ctx: Context)(posts: RDD[String]): RDD[Set[Int]]={
    val hashes: RDD[Set[Int]] = posts.map(post => shingleSet(post, ctx.shingleSize))
    hashes
  }


//  def shingleDocument(document: RDD[String]): RDD[Set[Int]] = {
//    //Compute every shingle hash
////    val shingles = document.map(text => shingleSet(text, 9))
////    val allShingles = shingles.reduce(_ union _)
//
//  }


  /**
    * Converts a string to a set of shingles of length k
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
    * @param text
    * @param k
    * @param index
    * @return
    */
  def kShingle(text: String, k: Int, index: Int): String = {
    text.substring(index, Math.min(index+k, text.length))
  }

  def hashShingle(shingle: String): Int = {
    shingle.hashCode
  }

}
