package skiis2

import java.util.concurrent.Executors
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers
import scala.collection._
import java.util.concurrent.atomic.AtomicInteger


class ParMapSuite extends AnyWordSpec with Matchers {
  import Skiis._

  val r = new scala.util.Random

  "Skiis" should {

    val tortureLevel = Option(System.getenv("TORTURE")) map (_.toInt) getOrElse 1
    for (i <- 1 to tortureLevel) {
      val context = Skiis.newContext("ParMapSuite",
          parallelism = r.nextInt(8) + 8,
          queue = r.nextInt(100) + 1,
          batch = r.nextInt(10) + 1
      )

      ("parMap %d" format i) in {
        val acc1 = new AtomicInteger()
        val acc2 = new AtomicInteger()
        val acc3 = new AtomicInteger()
        val acc4 = new AtomicInteger()

        val skiis1 = Skiis(1 to 1000 grouped 25) flatMap { batch =>
          acc1.incrementAndGet()
          Thread.sleep(r.nextInt(50))
          val b = batch map { x => (x, x + 1) }
          Skiis(b)
        }

        val skiis = skiis1.parMap { case (x1, x2) =>
          acc2.incrementAndGet()
          Thread.sleep(r.nextInt(50))
          (x1, x2, x1)
        }(context)

        skiis foreach { case (x1, x2, x3) =>
          x2 should === ((x1 + 1))
          x3 should === ((x1))
          acc3.incrementAndGet()
          acc4.addAndGet(x1)
        }

        acc1.get should === (40)
        acc2.get should === (1000)
        acc3.get should === (1000)
        acc4.get should === ((1 to 1000).sum)
      }

      ("parFlatMap %d" format i) in {
        val skiis = Skiis(1 to 1000).parFlatMap { i =>
          Thread.sleep(r.nextInt(50))
          val len = i % 100
          Skiis(1 to len)
        }(context)
        skiis.force().sum should === (1666500)
      }
    }
  }
}
