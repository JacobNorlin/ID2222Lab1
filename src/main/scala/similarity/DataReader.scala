package similarity

import org.apache.spark.rdd.RDD
import utils._

/**
  * Created by Jacob on 08-Nov-16.
  */
object DataReader extends Pipeline[String, RDD[String]] {

  /**
    * Parses a directory of blogposts and creates an RDD of all posts
    * @param ctx
    * @param directory
    * @return
    */
  def run(ctx: Context)(directory: String): RDD[String] = {
    val sc = ctx.sc
    val logData = sc.wholeTextFiles(directory).cache()
    val postPattern = "<post>(?s).*</post>".r
    val posts = logData.map({
      case (path, file) => {
        postPattern findFirstIn file get
      }
    })
    posts
  }

}
