package similarity

/**
  * Created by Jacob on 07-Nov-16.
  */
object CompareSets {
  def jaccardSimilarity(a: Set[Int], b: Set[Int]) = {
    val intersection = a intersect b
    val union = a union b
    intersection.size / union.size
  }
}
