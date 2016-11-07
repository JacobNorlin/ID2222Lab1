/**
  * Created by Jacob on 07-Nov-16.
  */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Main {

  def main(args: Array[String]) = {
    val logFile = "./README.MD" // Should be some file on your system
    val conf = new SparkConf()
      .setAppName("Simple Application")
      .setMaster(("local"))
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }

}
