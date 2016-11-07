package similarity

import org.apache.spark.rdd.RDD

/**
  * Created by Jacob on 07-Nov-16.
  */
object Shingling {//extends Pipeline[RDD[String], RDD[IndexedSeq[String]]]{

  def run(): Unit ={

  }


  def shingleDocument(document: RDD[String]): RDD[Set[Int]] = {
    document map(text => shingleSet(text, 3))
  }

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
