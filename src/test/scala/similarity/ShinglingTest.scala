package similarity

import org.scalatest._
import similarity.Shingling
/**
  * Created by Jacob on 07-Nov-16.
  */
class ShinglingTest extends FlatSpec with Matchers{

  Shingling.kShingle("Hello", 2, 0) should be ("He")

}
