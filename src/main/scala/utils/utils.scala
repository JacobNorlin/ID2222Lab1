package utils

import org.apache.spark.SparkContext

///**
//  * Created by Jacob on 07-Nov-16.
//  */
abstract class Pipeline[-F, +T] {
  self =>

  def andThen[G](thenn: Pipeline[T, G]): Pipeline[F, G] = new Pipeline[F, G] {
    def run(ctx: Context)(v: F): G = {
      val first = self.run(ctx)(v)
      thenn.run(ctx)(first)
    }
  }

  def run(ctx: Context)(v: F): T

}

case class Context(
                    val sc: SparkContext,
                    val shingleSize: Int
                  )