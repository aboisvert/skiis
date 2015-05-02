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
    val context = Skiis.DefaultContext

    val big = Skiis.newContext("ParForeachSuite-big", parallelism = 10)

    "report exceptions in parForeach" in {

      def f(x: Int) = { throw new Exception("foo") }

      def testWithIterations(iterations: Int) = {
        (the [Exception] thrownBy {
          Skiis(1 to iterations).parForeach(i => f(i))(context)
        }).getMessage shouldBe "foo"
      }

      for (i <- 1 to 1000 by 10) testWithIterations(i)
    }

    val small = Skiis.newContext("ParForeachSuite-small", parallelism = 5)

    val tortureLevel = Option(System.getenv("TORTURE")) map (_.toInt) getOrElse 1

    for (i <- 1 to tortureLevel) {
      ("parForeach %d" format i) in {
        Skiis(1 to 10).parForeach({ s: Int =>
          for (j <- 1 to 100) {
            val n = r.nextInt(j)
            val acc = new java.util.concurrent.atomic.AtomicInteger()
            Skiis(1 to n).parForeach { i => acc.incrementAndGet(); Thread.sleep(r.nextInt(10)) }(small)
            if (j % 10 == 1) print(".")
            acc.get should be === n
          }

          println()
        })(big)
      }
    }
  }
}