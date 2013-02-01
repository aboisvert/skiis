package skiis

import java.util.concurrent.Executors

import org.scalatest.WordSpec
import org.scalatest.matchers.ShouldMatchers

import scala.collection._
import scala.collection.mutable.ArrayBuffer

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class FusionSuite extends WordSpec with ShouldMatchers {
  import Skiis._

  "Skiis" should {

    "fuse maps" in {
      val mapped = Skiis(1 to 4) map (_ * 2) map (_ * 2)
      mapped.isInstanceOf[Skiis.MapOp[_]] should be === true
      mapped.toIterator.toList should be === Seq(4, 8, 12, 16)
    }

    "fuse with flatMap" in {
      val mapped = Skiis(1 to 4) map (_ * 2) flatMap { i => Skiis(i, i+1) }
      mapped.isInstanceOf[Skiis.FlatMapOp[_]] should be === true
      mapped.toIterator.toList should be === Seq(2, 3, 4, 5, 6, 7, 8, 9)
    }

    "fuse with multiple flatMap" in {
      val mapped = (Skiis(1 to 4)
        map (_ * 2)
        flatMap { i => Skiis(i, i+1) }
        flatMap { i => Skiis.singleton(i) }
        map identity
      )
      mapped.isInstanceOf[Skiis.FlatMapOp[_]] should be === true
      mapped.toIterator.toList should be === Seq(2, 3, 4, 5, 6, 7, 8, 9)
    }

    "fuse with filter" in {
      val filtered = (Skiis(1 to 4)
        flatMap { i => Skiis(i, i + 1) }
        filter { i => i >= 3 }
      )
      filtered.isInstanceOf[Skiis.FlatMapOp[_]] should be === true
      filtered.toIterator.toList should be === Seq(3, 3, 4, 4, 5)
    }

    "fuse with collect" in {
      val collected = (Skiis(1 to 4)
        flatMap { i => Skiis(i, i + 1) }
        collect { case i if (i >= 3) => i * 2 }
      )
      collected.isInstanceOf[Skiis.FlatMapOp[_]] should be === true
      collected.toIterator.toList should be === Seq(6, 6, 8, 8, 10)
    }

    "fuse and watch with collect" in {
      val intermediate = new ArrayBuffer[Int]()
      val collected = (Skiis(1 to 4)
        flatMap { i => Skiis(i, i + 1) }
        watch { i => intermediate += i; () }
        collect { case i if (i >= 3) => i * 2 }
      )
      collected.isInstanceOf[Skiis.FlatMapOp[_]] should be === true
      collected.toIterator.toList should be === Seq(6, 6, 8, 8, 10)
      intermediate should be === Seq(1, 2, 2, 3, 3, 4, 4, 5)
    }

  }
}