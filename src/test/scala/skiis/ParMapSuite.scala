package skiis

import java.util.concurrent.Executors
import org.scalatest.WordSpec
import org.scalatest.matchers.ShouldMatchers
import scala.collection._
import java.util.concurrent.atomic.AtomicInteger

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class ParMapSuite extends WordSpec with ShouldMatchers {
  import Skiis._

  val r = new scala.util.Random

  "Skiis" should {

    for (i <- 1 to 10) {
      implicit val context = new Skiis.Context {
        override lazy val parallelism = r.nextInt(8) + 8
        override lazy val queue = r.nextInt(100) + 1
        override lazy val batch = r.nextInt(10) + 1
        override lazy val executor = Skiis.newFixedThreadPool("ParMapSuite", threads = parallelism + 1)
      }

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

        val skiis = skiis1 parMap { case (x1, x2) =>
          acc2.incrementAndGet()
          Thread.sleep(r.nextInt(50))
          (x1, x2, x1)
        }

        skiis foreach { case (x1, x2, x3) =>
          x2 should be === (x1 + 1)
          x3 should be === (x1)
          acc3.incrementAndGet()
          acc4.addAndGet(x1)
        }

        acc1.get should be === 40
        acc2.get should be === 1000
        acc3.get should be === 1000
        acc4.get should be === (1 to 1000).sum
      }
    }
  }
}