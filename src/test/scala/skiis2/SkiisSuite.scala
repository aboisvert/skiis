package skiis2

import java.util.concurrent.Executors

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import scala.collection._
import scala.util._
import scala.language.postfixOps
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import java.util.concurrent.TimeUnit
import org.scalatest.Succeeded


class SkiisSuite extends AnyWordSpec with Matchers {
  import Skiis._

  val rnd = new java.util.Random()
  def random(x: Int) = math.abs(rnd.nextInt % x)


  "Skiis" should {
    val context = Skiis.DefaultContext copy (parallelism = 20)

    "map" in {
      val mapped = Skiis(1 to 10) map (_ * 2)
      mapped.toIterator.toSeq should === (Seq(2, 4, 6, 8, 10, 12, 14, 16, 18, 20))
    }

    "flatMap" in {
      val mapped = Skiis(1 to 4) flatMap { i => Skiis(Seq(i, i+1)) }
      mapped.toIterator.toSeq should === (Seq(1, 2, 2, 3, 3, 4, 4, 5))
    }

    "flatMap nothing" in {
      val mapped = Skiis(1 to 4) flatMap { i => Skiis(Seq.empty) }
      mapped.toIterator.toSeq should === (Seq())
    }

    "filter" in {
      val filtered = Skiis(1 to 10) filter (_ % 2 == 0)
      filtered.toIterator.toSeq should === (Seq(2, 4, 6, 8, 10))
    }

    "filterNot" in {
      val filtered = Skiis(1 to 10) filterNot (_ % 2 == 0)
      filtered.toIterator.toSeq should === (Seq(1, 3, 5, 7, 9))
    }

    "collect" in {
      val collected = Skiis(1 to 10) collect { case i if i % 2 == 0 => i+1 }
      collected.toIterator.toList should === (Seq(3, 5, 7, 9, 11))
    }

    "foreach" in {
      val acc = new java.util.concurrent.atomic.AtomicInteger()
      Skiis(Seq.fill(100)(1)) foreach { i => acc.incrementAndGet() }
      acc.get should === (100)
    }

    "for comprehension" in {
      val result = for (i <- Skiis(1 to 5); j <- Skiis(2 to 4) if i > j) yield (i, j)
      // val result = Skiis(1 to 10) flatMap { i => Skiis(1 to 2) map { j => (i, j) } }
      result.toIterator.toList should === (Seq((3,2), (4,2), (4,3), (5,2), (5,3), (5,4)))
    }

    "take" in {
      (Skiis(1 to 10) take 0)  should === (Seq())
      (Skiis(1 to 10) take 1)  should === (Seq(1))
      (Skiis(1 to 10) take 4)  should === (Seq(1,2,3,4))
      (Skiis(1 to 10) take 10) should === (Seq(1,2,3,4,5,6,7,8,9,10))
      (Skiis(1 to 10) take 11) should === (Seq(1,2,3,4,5,6,7,8,9,10))
    }

    "force" in {
      Skiis(1 to 10).force()  should === (Seq(1 to 10: _*))
    }

    "takeWhile" in {
      Skiis(1 to 10).takeWhile(_ <  1).toIterator.toSeq  should === (Seq())
      Skiis(1 to 10).takeWhile(_ <= 1).toIterator.toSeq  should === (Seq(1))
      Skiis(1 to 10).takeWhile(_ <  4).toIterator.toSeq  should === (Seq(1,2,3))
      Skiis(1 to 10).takeWhile(_ <= 10).toIterator.toSeq should === (Seq(1,2,3,4,5,6,7,8,9,10))
      Skiis(1 to 10).takeWhile(_ <= 11).toIterator.toSeq should === (Seq(1,2,3,4,5,6,7,8,9,10))
    }

    "lookahead" in {
      val counter = new java.util.concurrent.atomic.AtomicInteger()
      val skiis = Skiis(Iterator.continually { counter.incrementAndGet() } ).lookahead(queue = 1)

      Thread.sleep(100)
      counter.get shouldBe 2
      skiis.next() shouldBe Some(1)

      Thread.sleep(100)
      counter.get shouldBe 3
      skiis.next() shouldBe Some(2)
    }

    "grouped" in {
      (Skiis(1 to 3) grouped 1).toIterator.toSeq  should === (Seq(Seq(1), Seq(2), Seq(3)))
      (Skiis(1 to 4) grouped 2).toIterator.toSeq  should === (Seq(Seq(1, 2), Seq(3, 4)))
      (Skiis(1 to 5) grouped 2).toIterator.toSeq  should === (Seq(Seq(1, 2), Seq(3, 4), Seq(5)))
      (Skiis(1 to 1000) grouped 7).toIterator.toSeq  should === ((1 to 1000 grouped 7).toSeq)
    }

    def assertGroupedBy[K, V](skiis: Skiis[(K, Seq[V])], orderedHead: Seq[(K, Seq[V])], unorderedTail: Set[(K, Seq[V])]) = {
      val seq = skiis.to(Vector)
      //assert(seq.size == (orderedHead.size + unorderedTail.size))
      assert(seq.take(orderedHead.size) == orderedHead)
      assert(seq.takeRight(unorderedTail.size).toSet == unorderedTail)
    }

    "groupedBy1" in {
      assertGroupedBy(
        Skiis(1 to 10).groupedBy(maxGroupSize = 3, maxElements = Int.MaxValue, maxPartialGroups = Int.MaxValue)(_ % 2),
        orderedHead = Seq(
          (1, Seq(1, 3, 5)),
          (0, Seq(2, 4, 6))),
        unorderedTail = Set(
          (1, Seq(7, 9)),
          (0, Seq(8, 10))))
    }

    "groupedBy2" in {
      assertGroupedBy(
        Skiis(1 to 10).groupedBy(maxGroupSize = 3, maxElements = 4)(_ % 2),
        orderedHead = Seq(
          (0, Seq(2, 4)),
          (1, Seq(1, 3, 5)),
          (0, Seq(6, 8))),
        unorderedTail = Set((1, Seq(7, 9))))
    }

    "groupedBy3" in {
      assertGroupedBy(
        Skiis(Seq(1, 1, 2, 2, 2, 3, 3, 4)).groupedBy(maxGroupSize = 3, maxElements = 3)(_ % 3),
        orderedHead = Seq(
          (1, Seq(1, 1)),
          (2, Seq(2, 2, 2))),
        unorderedTail = Set(
          (0, Seq(3, 3)),
          (1, Seq(4))))
    }

    "groupedBy (with maxPartialGroups)" in {
      assertGroupedBy(
        Skiis(Seq(1, 1, 2, 2, 2, 3, 3, 4)).groupedBy(maxGroupSize = 3, maxElements = Int.MaxValue, maxPartialGroups = 2)(_ % 3),
        orderedHead = Seq(
          (1, Seq(1, 1)),
          (2, Seq(2, 2, 2))),
        unorderedTail = Set(
          (0, Seq(3, 3)),
          (1, Seq(4))))

      assertGroupedBy(
        Skiis(Seq(1, 1, 2, 2, 2, 3, 3, 4)).groupedBy(maxGroupSize = 3, maxElements = Int.MaxValue, maxPartialGroups = 3)(_ % 3),
        orderedHead = Seq(
          (2, Seq(2, 2, 2)),
          (1, Seq(1, 1, 4))),
        unorderedTail = Set((0, Seq(3, 3))))
    }

    "parForeach" in {
      val acc = new java.util.concurrent.atomic.AtomicInteger()
      Skiis(Seq.fill(100000)(1)).parForeach { i => acc.incrementAndGet() }(context)
      acc.get should === (100000)
    }

    "parMap" in {
      val mapped = Skiis(1 to 10).parMap (_ * 2)(context)
      mapped.toIterator.toSet should === (Set(2, 4, 6, 8, 10, 12, 14, 16, 18, 20))
    }

    "parMapWithQueue" in {
      val mapped = Skiis(1 to 10).parMapWithQueue[Int] { (x, q) => q += x * 2 }(context)
      mapped.toIterator.toSet should === (Set(2, 4, 6, 8, 10, 12, 14, 16, 18, 20))
    }

    "parFlatMap" in {
      val acc = new java.util.concurrent.atomic.AtomicInteger()
      val mapped = Skiis(Seq.fill(100000)(1)).parFlatMap { i => acc.incrementAndGet(); Skiis(i, i+1) }(context)
      mapped.toIterator.sum should === (300000)
      acc.get should === (100000)
    }

    "parFlatMap nothing" in {
      val mapped = Skiis(1 to 10000) flatMap { i => Skiis(Seq.empty) }
      mapped.toIterator.toSeq should === (Seq())
    }

    "parFilter" in {
      val filtered = Skiis(1 to 100000).parFilter { _ % 10 == 0 }(context)
      val result = filtered.toIterator.toSet
      result.size should === (100000/10)
      result should === ((10 to 100000 by 10).toSet)
    }

    "parFilterNot" in {
      val filtered = Skiis(1 to 100000).parFilterNot { _ % 10 != 0 }(context)
      filtered.toIterator.toSet should === ((10 to 100000 by 10).toSet)
    }

    "parCollect" in {
      val collected = Skiis(1 to 100000).parCollect { case i if i % 10 == 0 => i+1 }(context)
      collected.toIterator.toSet should === ((11 to 100001 by 10).toSet)
    }

    "parForce" in {
      Skiis(1 to 10).parForce(context).toSet  should === (Set(1 to 10: _*))
      Skiis(1 to 10).map (_ + 1 ).parForce(context).toSet  should === (Set(2 to 11: _*))
    }

    "combine parMap and parReduce" in {
      val reduceContext = Skiis.newContext("reduceContext", parallelism = 10)
      val result = Skiis(Seq.fill(10)(1)).parMap(_ * 2)(context).parReduce (_ + _)(reduceContext)
      result should === (20)
      reduceContext.executor.shutdown()
    }

    "combine parMap and parReduce with fixed thread pool of 5 threads" in {
      val context = Skiis.newContext("fixed 5 threads", parallelism = 5, queue = 10, batch = 1)
      val lock = new Object
      var max = 0
      var count = 0

      val mapped = Skiis(Seq.fill(10)(1)).parMap { i: Int =>
        //println("map: %d" format i)

        Thread.sleep(50)
        lock.synchronized {
          count += 1
        }
        Thread.sleep(50)

        lock.synchronized {
          max = if (count > max) count else max
        }

        Thread.sleep(100)
        lock.synchronized {
          count -= 1
        }
        i
      }(context)

      // mapped.toIterator.toList should === (Seq(1, 1, 1, 1, 1, 1, 1, 1, 1, 1))
      val reduce = mapped.parReduce { (i: Int, j: Int) =>
        // println("reduce: %d + %d" format (i, j))
        i + j
      }(context)
      reduce should === (10)
      count should === (0)
      max should === (5)
    }

    /*
    "reduce as mapped values become available" in {

      val pool1 = Executors.newFixedThreadPool(1)
      val lock = new Object
      val array = mutable.ArrayBuffer[Int]()
      var sum = 0

      val p = new Parallel(pool1, 10)
      val mapped = p.map(1 to 10 toSkiis) { (i: Int) =>
        lock.synchronized {
          array += i
        }
        i
      }
      val reduce = p.reduce(mapped) { (i: Int, j: Int) =>
        lock.synchronized {
          Console println("i="+i+" j="+j)

          if (array.size == 2) {
            i should === (array(0))
            j should === (array(1))
            sum = (i + j)
          } else {
            i should === (sum)
            j should === (array(array.size-1))
            sum += j
          }
        }
        i + j
      }
      reduce should === ((1 to 10).reduceLeft(_ + _))
      array.size should === (10)
    }
    */

    "work with Iterator" in {
      Skiis(Iterator(1,2,3)).parReduce ((_: Int) + (_: Int))(context) should === (6)
    }

    "work with large number of elements" in {
      val mapped = Skiis(Seq.fill(100000)(1)) map { _ * 2}
      val reduced = mapped.parReduce { _ + _ }(context)
      // mapped.toIterator.toList
      reduced should === (200000)
    }

    "zip" in {
      locally {
        // left side termination
        val s1 = Skiis(1 to 3)
        val s2 = Skiis(1 to 4)
        (s1 zip s2).toIterator.toList should === (List((1,1), (2,2), (3, 3)))
      }

      locally {
        // right side termination
        val s1 = Skiis(1 to 4)
        val s2 = Skiis(1 to 3)
        (s1 zip s2).toIterator.toList should === (List((1,1), (2,2), (3, 3)))
      }
    }

    "zipWithIndex" in {
      val s = Skiis("a", "b", "c")
      s.zipWithIndex.toIterator.toList should === (List(("a", 0), ("b", 1), ("c", 2)))
    }

    "concat using ++" in {
      val s1 = Skiis("a", "b", "c")
      val s2 = Skiis("d", "e")
      (s1 ++ s2).toIterator.toList should === (List("a", "b", "c", "d", "e"))
    }

    "merge/interleave several Skiis" in {
       Skiis.merge(Skiis(1,2,3), Skiis(4,5), Skiis(6,7,8,9)).toIterator.toList should === (List(1,4,6,2,5,7,3,8,9))
    }

    "merge two Skiis" in {
       (Skiis(1, 2, 3) merge Skiis(4, 5))to(List) should === (List(1,4,2,5,3))
    }

    "fanout" in {
      val set = (1 to 10000).toSet
      val skiis = Skiis(set) fanout (queues = 3, queueSize = 1)

      val futures = skiis
        .map { skii => Skiis.async { Try { skii.parPull(context).to(Set) shouldBe set } } }
        .to(Seq);

      //futures foreach { f => f.onComplete { x => x shouldBe Success(Success()) }(Skiis.executionContext) }
      futures.foreach { f =>
        val result = Await.result(f, Duration(10, TimeUnit.SECONDS))
        assert(result == Success(Succeeded))
      }

    }

    "parFold" in {
      val context = Skiis.newContext(name = "parFold", parallelism = 5)
      val result = Skiis(1 to 100)
        .parFold { i => (i, 0) }
                 { case ((index, total), x) => (index, total + x) }(context)
        .to(Seq)

      val indexes = result map (_._1)
      val totals = result map (_._2)

      indexes.toSet shouldBe (1 to 5).toSet
      totals.sum shouldBe (1 to 100).sum
    }

    "parFoldWithQueue" in {
      val fixtures = new FoldMapFixtures
      import fixtures._

      val context = Skiis.newContext(name = "parFoldWithQueue", parallelism = 5)
      val result = Skiis(1 to 100)
        .parFoldWithQueue[State, Int]
            /* init */    { i => init(i) }
            /* foldWithQueue */ { (i, x, q) => expectInit(i); yieldRandom() foreach { q += }; updateInit(i) }
            /* dispose */ { (i, q) => disposeInit(i); yieldRandom() foreach { q += } } (context)
        .to(Seq)
      assertFold(result)
    }

    "parFoldMap" in {
      val fixtures = new FoldMapFixtures
      import fixtures._

      val context = Skiis.newContext(name = "inject", parallelism = 5)
      val result = Skiis(1 to 10)
        .parFoldMap
            /* init */    { i => init(i) }
            /* foldMap */ { case (i, x) => expectInit(i); (updateInit(i), yieldRandom()) }
            /* dispose */ { i => disposeInit(i); yieldRandom() } (context)
        .to(Seq)
      assertFold(result)
    }

    "run previous computations seriallly when using `serialize`" in {
      var total = 0
      val result = Skiis(1 to 10000)
        .parMap (_ + 1)(context)
        .map { x => total += 1; x }
        .serialize()
        .parMap (_ + 1)(context)
        .to(Iterator)
        .sum
      total shouldBe 10000
      result shouldBe 50025000
    }

    "pull previous computations and store results in a queue" in {
      var total = 0
      val result = Skiis(1 to 10000)
        .parMap (_ +1)(context)
        .map { x => total += 1; x - total }
        .pull(queueSize = 10)
        .parMap (_ + 1)(context)
        .to(Iterator)
        .sum
      total shouldBe 10000
      result shouldBe 20000
    }

    "async success" in {
      val future = Skiis.async { 1 }
      future.onComplete {
        case Success(x) => x shouldBe 1
        case Failure(t) => fail("Expected success")
      }(Skiis.executionContext)
    }

    "async failure" in {
      val exception = new RuntimeException("foo")
      val future = Skiis.async { throw exception; 1 }
      future.onComplete {
        case Failure(x) => x shouldBe exception
        case Success(x) => fail("Expected an exception")
      }(Skiis.executionContext)
    }

    "execute a side-effect using `andThen`" in {
      @volatile var executed = false
      val total = Skiis(1 to 10000)
        .parMap (_ + 1)(context)
        .andThen { executed = true }
        .to(Iterator)
        .sum
      total shouldBe 50015000
      executed shouldBe true
    }
  }

  class FoldMapFixtures {
    val inited   = mutable.Map[Int, State]()
    val yielded  = mutable.ArrayBuffer[Int]()
    val folded   = mutable.ArrayBuffer[Int]()
    val disposed = mutable.ArrayBuffer[Int]()

    case class State(index: Int, var value: Int)

    def init(i: Int) = synchronized {
      val value = State(i, 0)
      inited(i) = value
      value
    }

    def expectInit(actual: State) = synchronized {
      actual shouldBe inited(actual.index)
    }

    def updateInit(current: State) = synchronized {
      val newValue = State(current.index, current.value + 1)
      inited(current.index) = newValue
      newValue
    }

    def disposeInit(s: State) = synchronized {
      disposed += s.index
    }

    def randomFlatMap(): Seq[Int] = {
      val len = random(5)
      for (i <- 1 to len) yield random(100)
    }

    def yieldRandom() = synchronized {
      val values = randomFlatMap()
      yielded ++= values
      values
    }

    def assertFold(actual: Seq[Int]) = synchronized {
      assert(disposed.size == inited.to(Seq).size)
      assert(disposed.sorted == inited.keys.toSeq.sorted)
      assert(actual.size == yielded.size)
      assert(actual.sorted == yielded.sorted)
    }
  }
}
