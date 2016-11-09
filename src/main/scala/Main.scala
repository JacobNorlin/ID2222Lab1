package similarity

/**
  * Created by Jacob on 07-Nov-16.
  */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import similarity.Shingling
import utils._

object Main {

  def main(args: Array[String]): Unit = {
    val logFile = "./documents" // Should be some file on your system
    System.setProperty("hadoop.home.dir", "C:\\Users\\Jacob\\Programmering\\lib\\hadoop-2.7.1")
    val conf = new SparkConf()
      .setAppName("Simple Application")
      .setMaster(("local"))
    val sc = new SparkContext(conf)
    val ctx = new Context(sc, 9)

    val pipeline = DataReader andThen Shingling andThen MinHashing andThen CompareSignatures
    pipeline.run(ctx)(logFile) foreach println

  }

}
