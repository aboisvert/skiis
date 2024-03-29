package skiis2

import java.util.concurrent.Executors

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import scala.collection._
import scala.collection.mutable.ArrayBuffer

// @org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class FusionSuite extends AnyWordSpec with Matchers {
  import Skiis._

  "Skiis" should {

    "fuse maps" in {
      val mapped = Skiis(1 to 4) map (_ * 2) map (_ * 2)
      mapped.isInstanceOf[Skiis.MapOp[_, _]] should === (true)
      mapped.toIterator.toList should === (Seq(4, 8, 12, 16))
    }

    "fuse with flatMap" in {
      val mapped = Skiis(1 to 4) map (_ * 2) flatMap { i => Skiis(i, i+1) }
      mapped.isInstanceOf[Skiis.FlatMapOp[_]] should === (true)
      mapped.toIterator.toList should === (Seq(2, 3, 4, 5, 6, 7, 8, 9))
    }

    "fuse with multiple flatMap" in {
      val mapped = (Skiis(1 to 4)
        map (_ * 2)
        flatMap { i => Skiis(i, i+1) }
        flatMap { i => Skiis(i) }
        map identity
      )
      mapped.isInstanceOf[Skiis.FlatMapOp[_]] should === (true)
      mapped.toIterator.toList should === (Seq(2, 3, 4, 5, 6, 7, 8, 9))
    }

    "fuse with filter" in {
      val filtered = (Skiis(1 to 4)
        flatMap { i => Skiis(i, i + 1) }
        filter { i => i >= 3 }
      )
      filtered.isInstanceOf[Skiis.FlatMapOp[_]] should === (true)
      filtered.toIterator.toList should === (Seq(3, 3, 4, 4, 5))
    }

    "fuse with collect" in {
      val collected = (Skiis(1 to 4)
        flatMap { i => Skiis(i, i + 1) }
        collect { case i if (i >= 3) => i * 2 }
      )
      collected.isInstanceOf[Skiis.FlatMapOp[_]] should === (true)
      collected.toIterator.toList should === (Seq(6, 6, 8, 8, 10))
    }

    "fuse and listen with collect" in {
      val intermediate = new ArrayBuffer[Int]()
      val collected = (Skiis(1 to 4)
        flatMap { i => Skiis(i, i + 1) }
        listen { i => intermediate += i; () }
        collect { case i if (i >= 3) => i * 2 }
      )
      collected.isInstanceOf[Skiis.FlatMapOp[_]] should === (true)
      collected.toIterator.toList should === (Seq(6, 6, 8, 8, 10))
      intermediate should === (Seq(1, 2, 2, 3, 3, 4, 4, 5))
    }

  }
}
