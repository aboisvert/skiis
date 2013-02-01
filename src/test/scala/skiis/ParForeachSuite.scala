package skiis

import java.util.concurrent.Executors

import org.scalatest.WordSpec
import org.scalatest.matchers.ShouldMatchers

import scala.collection._

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class ParForeachSuite extends WordSpec with ShouldMatchers {
  import Skiis._

  val r = new scala.util.Random

  "Skiis" should {
    implicit val context = Skiis.DefaultContext

    val big = new Skiis.Context {
      override val parallelism = 10
      override val queue = 1
      override val batch = 1
      override val executor = Executors.newFixedThreadPool(parallelism)
    }

    for (i <- 1 to 100) {
      ("parForeach %d" format i) in {
        Skiis(1 to 10).parForeach({ s: Int =>
          for (j <- 1 to 100) {
            val n = r.nextInt(j)
            val acc = new java.util.concurrent.atomic.AtomicInteger()
            Skiis(1 to n) parForeach { i => acc.incrementAndGet(); Thread.sleep(r.nextInt(10)) }
            if (j % 10 == 1) print(".")
            acc.get should be === n
          }

          println()
        })(big)
      }
    }
  }
}