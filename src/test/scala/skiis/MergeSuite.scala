package skiis

import java.util.concurrent.Executors

import org.scalatest.WordSpec
import org.scalatest.matchers.ShouldMatchers

import scala.collection._

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class MergeSuite extends WordSpec with ShouldMatchers {
  import Skiis._

  val r = new scala.util.Random

  def random(n: Int) = math.abs(r.nextInt) % n

  "Skiis" should {
    implicit val context = Skiis.DefaultContext

    val big = new Skiis.Context {
      override val parallelism = 10
      override val queue = 1
      override val batch = 1
      override val executor = Executors.newFixedThreadPool(parallelism)
    }

    // torture-test Skiis.merge()
    for (i <- 1 to 1000) {
      ("merge %d" format i) in {
        val ranges = for (r <- 1 to (random(100) + 10)) yield (1 to (r + random(10000)))
        val skiis = ranges map { r => Skiis.apply(r.iterator) }

        val expectedTotal = ranges map (_.sum) sum

        val merged = Skiis.merge(skiis: _*)

        val actualTotal = new java.util.concurrent.atomic.AtomicInteger()
        val n = new java.util.concurrent.atomic.AtomicInteger()
        merged parForeach { x =>
          if (random(1000) == 0) Thread.sleep(1)
          actualTotal.addAndGet(x)
          n.incrementAndGet()
        }

        actualTotal.get should be === expectedTotal
        println("total adds: " + n.get)
      }
    }
  }
}