package skiis2

import java.util.concurrent.Executors
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers
import scala.collection._
import java.util.concurrent.atomic.AtomicInteger

//@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class ParFlatMapSuite extends AnyWordSpec with Matchers {
  import Skiis._

  val r = new scala.util.Random // thread-safe

  "Skiis" should {
    val context = Skiis.newContext("ParMapSuite", parallelism = 4, queue = 100, batch = 1)

    def randomInt(x: Int): Int = {
      math.abs(r.nextInt % x)
    }

    val tortureLevel = Option(System.getenv("TORTURE")) map (_.toInt) getOrElse 1

    for (loop <- 1 to tortureLevel) {
      s"parFlatMap followed by grouped - loop $loop" in {
      val acc = new java.util.concurrent.atomic.AtomicInteger()
      val ctx2 = Skiis.newContext("ParMapSuite2", parallelism = 3, queue = 10, batch = 2)
      Skiis(1 to 999)
         .parFlatMap { x =>
           if (randomInt(10) % 5 == 0) {
             Thread.sleep(randomInt(3));
             Skiis(Seq.fill(10)(x))
           } else {
             Skiis.empty
           }
         }(context)
         .grouped(7)
         .parForeach { xs => xs foreach { x => acc.addAndGet(x) }; Thread.sleep(randomInt(3))   }(ctx2)
      }
    }
  }
}
