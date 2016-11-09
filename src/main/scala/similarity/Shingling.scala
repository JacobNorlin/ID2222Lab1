package similarity

import org.apache.spark.rdd.RDD
import utils._
/**
  * Created by Jacob on 07-Nov-16.
  */
object Shingling extends Pipeline[RDD[String], RDD[Set[Long]]]{
  /**
    * Takes an RDD of that that contains every document as as a string.
    * Converts every document to a set of hashed shingles.
    * @param ctx
    * @param posts
    * @return
    */
  def run(ctx: Context)(posts: RDD[String]): RDD[Set[Long]]={
    val shingleSize = ctx.shingleSize
    val hashes: RDD[Set[Long]] = posts.map(post => shingleSet(post, shingleSize))
    hashes
  }

//
//  def shingleDocument(documents: RDD[String]): RDD[Set[Long]] = {
//    //Compute every shingle hash
//    val shingles = documents.map(text => shingleSet(text, 9))
//    val allShingles = shingles.reduce(_ union _).flatten
//
//    val cMatrix = shingles.map(shingles => {
//      shingles.map(shingle => {
//        allShingles
//      })
//    })
//
//    cMatrix
//  }


  /**
    * Converts a string to a set of shingles of length k
    * @param text
    * @param k
    * @return
    */
  def shingleSet(text: String, k: Int): Set[Long] = {
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

  def hashShingle(shingle: String): Long = {
    shingle.hashCode
  }

}
