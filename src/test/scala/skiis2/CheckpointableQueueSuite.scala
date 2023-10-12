package skiis2

import java.util.concurrent.Executors
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers
import scala.collection._
import scala.collection.mutable.ArrayBuffer
import scala.language.{ postfixOps, reflectiveCalls }
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

// @org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class CheckpointableQueueSuite extends AnyWordSpec with Matchers {
  import Skiis._

  "CheckpointableQueue" should {
    val context = Skiis.DefaultContext

    "accept elements and process them immediately" in {
      val latch = new CountDownLatch(1)
      val queue = Skiis.newCheckpointableQueue[Int]("CheckpointableQueueSuite-1", 10) { xs =>
        xs foreach { x => latch.countDown() }
      }
      queue += 1
      // note: we do not use checkpoint() here
      latch.await(1, TimeUnit.SECONDS)
    }

    "accept (lots of) elements and process them immediately" in {
      val n = 1000
      val latch = new CountDownLatch(n)
      val sum = new AtomicInteger(0)
      val queue = Skiis.newCheckpointableQueue[Int]("CheckpointableQueueSuite-1", 10) { xs =>
        xs
          .grouped(10)
          .parForeach { batch => latch.countDown(); sum.addAndGet(batch.sum) }(context)
      }

      for (i <- 1 to n) { queue += i }
      queue.checkpoint()
      sum.get shouldBe (1 to n).sum
    }

    "work with successive checkpoints" in {
      val n = 1000
      val sum = new AtomicInteger(0)
      val queue = Skiis.newCheckpointableQueue[Int]("CheckpointableQueueSuite-1", 10) { xs =>
        xs
          .grouped(11)
          .parForeach { batch => sum.addAndGet(batch.sum) }(context)
      }

      for (i <- 1 to n) { queue += i }
      queue.checkpoint()
      sum.get shouldBe (1 to n).sum

      for (i <- 1 to n) { queue += i }
      queue.checkpoint()
      sum.get shouldBe ((1 to n).sum * 2)

      for (i <- 1 to n) { queue += i }
      queue.checkpoint()
      sum.get shouldBe ((1 to n).sum * 3)
    }
  }
}
