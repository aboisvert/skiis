package skiis2

import java.util.concurrent._
import java.util.concurrent.atomic._
import java.util.concurrent.locks._

import scala.annotation.tailrec
import scala.annotation.unchecked.uncheckedVariance
import scala.collection.mutable.ArrayBuffer
import scala.collection.generic.CanBuildFrom
import scala.concurrent.{ExecutionContext, Future, Promise}

/** "Parallel Skiis"
 *
 *  Thread-safe and resource-aware iterator-like collections. All subclasses *must* be thread-safe.
 *
 *  Since Skiis's are meant to be used concurrently, their ordering is generally undefined.
 *
 *  The regular map(), flatMap(), filter(), etc. functions are lazy and meant to be used for
 *  stream-fusion.
 *
 *  The parMap(), parFlatMap(), parFilter(), etc. methods are parallel operations leveraging
 *  an executor and desired level of parallelism.  See Skiis.Context.
 */
trait Skiis[+T] extends { self =>
  import Skiis._

  /** Return the next element of this collection. */
  def next(): Option[T]

  /** Return the next `n` elements of this collection. */
  def take(n: Int): Seq[T] = {
    val buf = new ArrayBuffer[T](n)
    take(n, buf)
    buf
  }

  def take[TT >: T](n: Int, buffer: ArrayBuffer[TT]): Unit = {
    buffer.clear()
    var i = 0
    while (i < n) {
      val n = next()
      if (n.isEmpty) return
      buffer += n.get
      i += 1
    }
  }

  /** Return elements while `f` predicate is satisfied. */
  def takeWhile(f: T => Boolean): Skiis[T] = new Skiis[T] {
    private var done = false
    def next(): Option[T] = synchronized {
      if (done) return None
      val n = self.next()
      if (n.isEmpty) { done = true; return None }
      else if (f(n.get)) return n
      else { done = true; return None }
    }
  }

  /** Group stream into groups of `n` elements */
  def grouped(n: Int): Skiis[Seq[T]] = new Skiis[Seq[T]] {
    override def next(): Option[Seq[T]] = {
      val group = self.take(n)
      if (group.isEmpty) None else Some(group)
    }
  }

  /** Transform elements of this collection using `f` and return a new collection. */
  def map[U](f: T => U): Skiis[U] = {
    self match {
      case map: MapOp[_, T @unchecked] => map compose f

      case flatMap: FlatMapOp[T @unchecked]  =>
        flatMap compose { previous => new ApplyContinuation[T, U] {
          override def apply(x: T, continuation: U => Unit) {
            previous(x, (x: T) => continuation(f(x))) // possibly fusion
          }
        }}

      case _ =>
        val capturedF = f
        new MapOp[T, U] {
          override var previous = self
          override var f = capturedF
        }
    }
  }

  def listen[U](f: T => Unit): Skiis[T] = map { x =>
    try {
      f(x)
    } catch {
      case e: Exception => ()
    }
    x
  }

  /** Transform elements of this collection with the function `f` producing zero-or-more
   *  outputs per input element and return a new collection concatenating the outputs.
   */
  def flatMap[U](f: T => Skiis[U]): Skiis[U] = {
    self match {
      case flatMap: FlatMapOp[T @unchecked] =>
        flatMap compose { previous => new ApplyContinuation[T, U] {
          override def apply(x: T, continuation: U => Unit) {
            previous(x, f(_) foreach continuation) // possibly fusion
          }
        }}

      case _ =>
        new Skiis[U] with FlatMapOp[U] {
          override var applyAndRunContinuation: ApplyContinuation[_, U] = new ApplyContinuation[T, U] {
            override def apply(x: T, continuation: U => Unit) {
              f(x) foreach continuation // possibly fusion
            }
          }
          override val previous: Skiis[T] = self
        }
    }
  }

  /** Selects all elements of this collection which satisfy a predicate. */
  def filter(f: T => Boolean): Skiis[T] = withFilter(f)

  /** Selects all elements of this collection which satisfy a predicate. */
  def withFilter(f: T => Boolean): Skiis[T] = {
    self match {
      case flatMap: FlatMapOp[T @unchecked] =>
        flatMap compose { previous => new ApplyContinuation[T, T] {
          override def apply(x: T, continuation: T => Unit) {
            previous(x, x => if (f(x)) continuation(x)) // possibly fusion
          }
        }}

      case _ =>
        new Skiis[T]() {
          override def next(): Option[T] = {
            while (true) {
              val next = self.next()
              if (next.isEmpty || f(next.get)) {
                return next
              }
            }
            sys.error("unreachable")
          }
        }
    }
  }

  /** Selects all elements of this collection which do not satisfy a predicate. */
  def filterNot(f: T => Boolean): Skiis[T] = filter(!f(_))

  /** Filter and transform elements of this collection using the partial function `f` */
  def collect[U](f: PartialFunction[T, U]): Skiis[U] = {
    self match {
      case flatMap: FlatMapOp[T @unchecked] =>
        flatMap compose { previous => new ApplyContinuation[T, U] {
          override def apply(x: T, continuation: U => Unit) {
            previous(x, x => if (f.isDefinedAt(x)) continuation(f(x))) // possibly fusion
          }
        }}

      case _ =>
        new Skiis[U]() {
          override def next(): Option[U] = {
            while (true) {
              val next = Skiis.this.next()
              if (next.isEmpty) return None
              if (f.isDefinedAt(next.get)) {
                return Some(f(next.get))
              }
            }
            sys.error("unreachable")
          }
        }
    }
  }

  /** Applies a function `f` to all elements of this collection */
  def foreach(f: T => Unit) {
    while (true) {
      val next = self.next()
      if (next.isEmpty) return
      f(next.get)
    }
  }

  /** Convert this collection into an Iterator */
  def toIterator: Iterator[T] = new Iterator[T] {
    private var current: Option[T] = null.asInstanceOf[Option[T]]
    private def load() = {
      if (current == null) current = Skiis.this.next()
      current
    }
    override def hasNext = load().isDefined
    override def next() = {
      val n = load()
      if (n.isEmpty) throw new IllegalStateException("No more elements")
      current = null
      n.get
    }
  }

  /** Force evaluation of previous lazy computation in a serialized (read: fully synchronized) context */
  def serialize(): Skiis[T] = Skiis(toIterator)

  /** "Pull" all values from the collection using a single (separate) thread, forcing evaluation of previous lazy computation,
   *    and enqueue them in a Skiis.Queue collection  of up to `queueSize` size.
   */
  def pull(queueSize: Int): Skiis[T] = {
    val queue = new Queue[T](queueSize)
    Skiis.async {
      this foreach { queue += _ }
      queue.close()
    }
    queue
  }

  /** "Pull" all values from the collection using the given context, forcing evaluation of previous lazy computation. */
  def parPull(context: Context): Skiis[T] = {
    val job = new Job[T](context) with Queuing[T] {
      override def process(t: T) = enqueue(t)
    }
    job.start()
    job
  }

  /** Force evaluation of previous lazy computations and discard all resulting values. */
  def discardAll(): Unit = {
    foreach { _ => () }
  }

  /** Force evaluation of the collection and convert this Skiis to a collection of type `Col` */
  def to[Col[_]](implicit cbf: CanBuildFrom[Nothing, T, Col[T @uncheckedVariance]]): Col[T @uncheckedVariance] = toIterator.to[Col]

  /** Force evaluation of the collection and return all elements in a strict (non-lazy) Seq. */
  def force(): Seq[T] = to[Vector]

  def parForce(context: Context): Seq[T] = {
    parPull(context).force()
  }

  /** Applies a function `f` in parallel to all elements of this collection */
  def parForeach(f: T => Unit)(context: Context) {
    parForeachAsync(f)(context).result // block for result
  }

  /** Applies a function `f` in parallel to all elements of this collection */
  @Experimental
  def parForeachAsync(f: T => Unit)(context: Context): Control with Result[Unit] = {
    val job = new Job[Unit](context) with Result[Unit] {
      private val completed = new Condition(lock)
      override def process(t: T) = f(t)
      override def notifyExceptionOrCancelled() = completed.signalAll()
      override def notifyWorkerCompleted() = startWorkers()
      override def notifyAllWorkersDone() = completed.signalAll()
      override def notifyPossiblyNoMore() = completed.signalAll()
      override def result = {
        lock.lock()
        try {
          while (!isDone) {
            bailOutIfNecessary()
            completed.await()
          }
          bailOutIfNecessary()
        } finally {
          lock.unlock()
        }
        ()
      }
    }

    job.start()
    job
  }

  /** Transform elements of this collection in parallel using `f` and return a new collection. */
  def parMap[U](f: T => U)(context: Context): Skiis[U] = this.map(f).parPull(context)

  /** Transform elements of this collection in parallel with the function `f`
   *  producing zero-or-more  outputs per input element and return a new collection
   *  concatenating all the outputs.
   */
  def parFlatMap[U](f: T => Skiis[U])(context: Context): Skiis[U] = this.flatMap(f).parPull(context)

  /** This is an optimized/procedural version of parFlatMap() where the `proc` produces directly to a queue. */
  def parMapWithQueue[U](proc: (T, Queue[U]) => Unit)(context: Context): Queue[U] = {
    val queue = new Queue[U](context.queue)
    Skiis.async {
      try {
        this.parForeach { proc(_, queue) }(context)
        queue.close()
      } catch { case t: Throwable =>
        queue.reportException(t)
      }
    }
    queue
  }

  /** Selects (in parallel) all elements of this collection which satisfy a predicate. */
  def parFilter(f: T => Boolean)(context: Context): Skiis[T] = this.filter(f).parPull(context)

  /** Selects (in parallel) all elements of this collection which do not satisfy a predicate. */
  def parFilterNot(f: T => Boolean)(context: Context): Skiis[T] = this.filterNot(f).parPull(context)

  /** Filter and transform elements of this collection (in parallel) using the partial function `f` */
  def parCollect[U](f: PartialFunction[T, U])(context: Context): Skiis[U] = this.collect(f).parPull(context)

  def parReduce[TT >: T](f: (TT, T) => TT)(context: Context): TT = {
    val job = new Job[T](context) with Result[TT] {
      private val acc = new AtomicReference[TT]()

      private val completed = new Condition(lock)

      override def process(t: T) {
        var current1 = t.asInstanceOf[TT]
        while (current1 != null) {
          bailOutIfNecessary()

          if (acc.compareAndSet(null.asInstanceOf[TT], current1)) {
            current1 = null.asInstanceOf[TT]
          } else {
            val current2 = acc.getAndSet(null.asInstanceOf[TT]).asInstanceOf[T]
            if (current2 != null) {
              current1 = f(current1, current2)
            }
          }
        }
      }

      override def notifyExceptionOrCancelled() = completed.signalAll()
      override def notifyPossiblyNoMore() = completed.signalAll()
      override def notifyWorkerCompleted() = startWorkers()
      override def notifyAllWorkersDone() = completed.signalAll()

      override def result = {
        lock.lock()
        try {
          while (!isDone) {
            bailOutIfNecessary()
            completed.await()
          }
          bailOutIfNecessary()
        } finally {
          lock.unlock()
        }
        acc.get
      }

    }
    job.start()
    job.result
  }

  def zipWithIndex: Skiis[(T, Long)] = {
    val counter = new AtomicLong()
    this map { t => (t, counter.getAndIncrement) }
  }

  def zip[U](other: Skiis[U]): Skiis[(T, U)] = new Skiis[(T, U)] {
    private var done = false
    override def next(): Option[(T, U)] = synchronized {
      if (done) return None

      val n1 = self.next()
      val n2 = other.next()
      if (n1.isDefined && n2.isDefined) Some((n1.get, n2.get))
      else {
        done = true
        None
      }
    }
  }

  /** See Skiis.merge(Skiis[T]*) */
  def merge[TT >: T](other: Skiis[TT]): Skiis[TT] = {
    Skiis.merge(this, other)
  }

  /** Parallel fold.
   *
   *  This method generates `context.parallel` initial values using the `initial` function, then
   *  concurrently folds all these values with values from this Skiis[T] and returns the resulting
   *  folded values.
   */
  def parFold[U: Manifest](init: Int => U)(fold: (U, T) => U)(context: Context): IndexedSeq[U] = {
    val vars = Array.tabulate(context.parallelism) { i => new Var(init(i+1)) }
    val ring = new ConcurrentRing(vars)
    (ring zip this).parForeach { case (v, t) =>
      v.synchronized { v.value = fold(v.value, t) }
    }(context)
    vars map (_.value)
  }

  /** Combination of parallel fold and flatMap used to carry stateful computations yielding values.
   *
   *  This method generates `context.parallel` initial values using the `initial` function, then
   *  concurrently folds all these values with values from this Skiis[T] and returns the resulting
   *  folded values.
   */
  def parFoldMap[U, V](init: Int => U)(foldMap: (U, T) => (U, Seq[V]))(dispose: U => Seq[V])(context: Context): Skiis[V] = {
    val queue = new Queue[V](context.queue)
    Skiis.async {
      val vars = try {
        Array.tabulate(context.parallelism) { i => new Var(init(i+1)) }
      } catch { case e: Exception =>
        queue.reportException(e);
        throw e
      }
      def disposeVars() = {
        Skiis(vars.iterator).parForeach { v =>
          val flatMapped = dispose(v.value)
          flatMapped foreach { queue += _ }
        }(context)
      }
      val ring = new ConcurrentRing(vars)
      try {
        (ring zip this).parForeach { case (v, t) =>
          val (newVar, flatMapped) = foldMap(v.value, t)
          v.value = newVar
          flatMapped foreach { queue += _ }
        } (context)
        disposeVars()
        queue.close()
      } catch { case t: Throwable =>
        queue.reportException(t)
        try { disposeVars() } catch { case e: Exception => e.printStackTrace() }
      }
    }
    queue
  }

  /** Combination of parallel fold and flatMap.
   *
   *  This method generates `context.parallel` initial values using the `initial` function, then
   *  concurrently folds all these values with values from this Skiis[T] and returns the resulting
   *  folded values.
   */
  def parFoldWithQueue[U, V](init: Int => U)(foldWithQueue: (U, T, Queue[V]) => U)(dispose: (U, Queue[V]) => Unit)(context: Context): Queue[V] = {
    val queue = new Queue[V](context.queue)
    Skiis.async {
      val vars = try {
        Array.tabulate(context.parallelism) { i => new Var(init(i+1)) }
      } catch { case e: Exception =>
        queue.reportException(e);
        throw e
      }
      def disposeVars() = {
        Skiis(vars.iterator).parForeach { v => dispose(v.value, queue) }(context)
      }
      val ring = new ConcurrentRing(vars)
      try {
        (ring zip this).parForeach { case (v, t) =>
          v.synchronized {
            val newVar = foldWithQueue(v.value, t, queue)
            v.value = newVar
          }
        } (context)
        vars foreach { v => dispose(v.value, queue) }
        queue.close()
      } catch { case t: Throwable =>
        queue.reportException(t)
        try { disposeVars() } catch { case e: Exception => e.printStackTrace() }
      }
    }
    queue
  }

  /** Fan-out this Skiis[T] into multiple queues by replicating each element to each fan-out queue. */
  def fanout[TT >: T](queues: Int, queueSize: Int = 1): IndexedSeq[Queue[TT]] = {
    val qq = for (i <- 1 to queues) yield new Queue[TT](queueSize)
    Skiis.async {
      this foreach { x =>
        var i = 0
        while (i < queues) {
          qq(i) += x
          i += 1
        }
      }
      for (q <- qq) q.close()
    }
    qq
  }

  /** Concatenate elements from another Skiis[T].
   *
   *  e.g., Skiis(1,2,3) ++ Skiis(4,5) => Skiis(1,2,3,4,5)
   */
  def ++[TT >: T](other: Skiis[TT]): Skiis[TT] = new Skiis[TT] {
    private var selfEmpty = false
    override def next(): Option[TT] = synchronized {
      if (!selfEmpty) {
        val n = self.next()
        if (n.isDefined) return n
        else selfEmpty = true
      }
      other.next()
    }
  }

  private[Skiis] class Var[T](var value: T)

  /** Job holds completion status and computation output */
  private[Skiis] abstract class Job[U](val context: Context) extends Control { job =>
    protected val lock = new ReentrantLock()
    protected var workersOutstanding = 0
    protected var cancelled = false
    protected var noMore = false
    protected var exception: Throwable = _

    private[Skiis] def start() {
      startWorkers()
    }

    protected def process(input: T @uncheckedVariance): Unit

    protected def bailOutIfNecessary() {
      lock.lock()
      try {
        if (exception != null) throw exception
        if (cancelled) throw new CancellationException("Parallel operation was cancelled")
      } finally {
        lock.unlock()
      }
    }

    protected def needMoreWorkers: Boolean = (workersOutstanding < context.parallelism)

    protected def startWorkers() {
      lock.lock()
      try {
        while (!noMore && needMoreWorkers) {
          bailOutIfNecessary()
          workersOutstanding += 1
          context.executor.submit(new Worker(context.batch))
        }
      } catch {
        case e: Throwable => reportException(e)
      } finally {
        lock.unlock()
      }
    }

    private[Skiis] def workerCompleted(): Unit = {
      lock.lock()
      try {
        workersOutstanding -= 1
        notifyWorkerCompleted()
        if (workersOutstanding <= 0) {
          notifyAllWorkersDone()
        }
      } finally {
        lock.unlock()
      }
    }

    protected def notifyExceptionOrCancelled(): Unit
    protected def notifyWorkerCompleted(): Unit
    protected def notifyAllWorkersDone(): Unit
    protected def notifyPossiblyNoMore(): Unit

    private def reportException(t: Throwable): Unit = {
      lock.lock()
      try {
        if (cancelled == false && exception == null) exception = t
        notifyExceptionOrCancelled()
      } finally {
        lock.unlock()
      }
    }

    /** Attempts to cancel execution of this task. */
    override def cancel(): Unit = {
      lock.lock()
      try {
        cancelled = true
        notifyExceptionOrCancelled()
      } finally {
        lock.unlock()
      }
    }

    /** Returns true if this task was cancelled before it completed normally. */
    def isCancelled: Boolean = {
      lock.lock()
      try {
        cancelled
      } finally {
        lock.unlock()
      }
    }

    /** Returns true if this task completed. Completion may be due to normal termination,
     *  an exception, or cancellation -- in all of these cases, this method will return true.
     */
    def isDone: Boolean = {
      lock.lock()
      try {
        (workersOutstanding <= 0 && noMore) || (exception != null) || cancelled
      } finally {
        lock.unlock()
      }
    }

    private class Worker(val batch: Int) extends Runnable {
      private[this] val buffer: ArrayBuffer[T] = {
        if (batch > 1) new ArrayBuffer[T](batch) else null
      }
      def run: Unit = {
        try {
          while (true) {
            bailOutIfNecessary()
            val shouldNotify = if (batch == 1) {
              val next = Skiis.this.next()
              if (next.isDefined) process(next.get)
              next.isEmpty
            } else {
              val next = Skiis.this.take(batch, buffer)
              val size = buffer.size
              var i = 0
              while (i < size) {
                process(buffer(i))
                i += 1
              }
              size < batch
            }
            if (shouldNotify) {
              lock.lock()
              try {
                noMore = true
                notifyPossiblyNoMore()
              } finally {
                lock.unlock()
              }
              return
            }
          }
        } catch {
          case ex: Throwable => job.reportException(ex)
        } finally {
          job.workerCompleted()
        }
      }
    }
  }

  trait Queuing[U] extends Skiis[U] { self: Job[U] =>
    private val results = new LinkedBlockingQueue[Option[U]](context.queue)

    private val available = new Condition(lock)

    protected final def enqueue(output: U): Unit = {
      results.put(Some(output))
      available.signalAll()
    }

    protected final def enqueue(outputs: Seq[U]): Unit = {
      outputs foreach { enqueue(_) }
    }

    override final def notifyExceptionOrCancelled() = available.signalAll()
    override final def notifyWorkerCompleted() = () // new workers started as-needed in next()
    override final def notifyAllWorkersDone() = available.signalAll()
    override final def notifyPossiblyNoMore() = available.signalAll()

    override protected def needMoreWorkers = {
      (workersOutstanding <= context.parallelism && results.size + (workersOutstanding * context.batch) < context.queue)
    }

    override def next: Option[U] = next(-1L, TimeUnit.MILLISECONDS)

    /** Waits if necessary for at most the given time for the computation to complete,
     *  and then retrieves its result, if available.
     */
    def next(timeout: Long, unit: TimeUnit): Option[U] = {
      val start = System.currentTimeMillis
      val deadline = if (timeout >= 0) {
        System.currentTimeMillis + unit.toMillis(timeout)
      } else {
        Long.MaxValue
      }
     lock.lock()
     try {
        while (workersOutstanding > 0 || !noMore || results.size > 0) {
          bailOutIfNecessary()
          startWorkers()
          val next = results.poll()
          if (next != null) {
            return next
          }
          available.await(deadline - System.currentTimeMillis)
        }
        bailOutIfNecessary()
      } finally {
        lock.unlock()
      }
      return None
    }
  }

  @Experimental
  private[Skiis] trait Result[U] {
    /** Block for result, may throw exception if underlying computation failed. */
    def result: U
  }

  /** A concurrent + mutable ring to continuously iterate through a set of elements.
   *
   *  The ring only supports removal and will return `null` when the ring becomes empty.
   */
  private[Skiis] class ConcurrentRing[T](ts: Seq[T]) extends Skiis[T] {
    private[this] val ref = new AtomicReference(ts.toArray[Any] map { x => Some(x) })
    private[this] val index = new AtomicInteger()

    /** Get next element in ring order (subject to concurrency non-determinism)
     *  or `null` if the ring is empty.
     */
    def next(): Option[T] = {
      val array = ref.get
      if (array.length == 0) return None
      val i = math.abs(index.getAndIncrement % array.length)
      array(i).asInstanceOf[Option[T]]
    }

    /** Remove element `t` from the ring */
    def remove(t: T): Unit = synchronized {
      val oldArray = ref.get
      val i = oldArray.indexOf(Some(t))
      if (i == -1) return
      val newArray = new Array[Some[Any]](oldArray.length - 1)
      System.arraycopy(oldArray, 0, newArray, 0, i)
      if (newArray.length > i) {
        System.arraycopy(oldArray, i + 1, newArray, i, newArray.length - i)
      }
      ref.set(newArray)
    }
  }

  /** A condition that implicitly locks/unlocks the underlying reentrant lock. */
  private[Skiis] class Condition(val lock: Lock = new ReentrantLock) {
    private val condition = lock.newCondition()

    def await() = {
      lock.lock()
      try {
        condition.await()
      } finally {
        lock.unlock()
      }
    }

    def await(timeout: Long) = {
      lock.lock()
      try {
        condition.await(timeout, TimeUnit.MILLISECONDS)
      } finally {
        lock.unlock()
      }
    }

    def signalAll() = {
      lock.lock()
      try {
        condition.signalAll()
      } finally {
        lock.unlock()
      }
    }
  }

}

object Skiis {

  /** Internal thread pool used to dispatch code submitted through async() method */
  private[skiis2] lazy val cachedThreadPool = newCachedThreadPool("Skiis")

  lazy val executionContext = new ExecutionContext {
    override def execute(runnable: Runnable): Unit = cachedThreadPool.submit(runnable)
    override def reportFailure(t: Throwable): Unit = t.printStackTrace()
    override def prepare(): ExecutionContext = this
  }

   /** Abstracts over the implementation details of processing elements,
    *  possibly through a chain of operations, including batching and such.
    *
    *  You can think of this as a function T => Seq[U] defined in continuation-passing style.
    */
  private[skiis2] trait ApplyContinuation[-T, +U] {
    /** Processes the next element `x` and invokes `continuation` for each resulting element. */
    def apply(x: T, continuation: U => Unit): Unit
  }

  /** Convenient to generate universally-quantified continuation types */
  private[skiis2] type Continuation[U] = ApplyContinuation[Any, U]

  /** Coerse existentially-quantified continuations into universally-quantified continuations */
  @inline
  private[skiis2] def universal[U](x: ApplyContinuation[_, U]) = x.asInstanceOf[Continuation[U]]

  /** Returns a ThreadFactory that creates named + numbered daemon threads */
  def newDaemonThreadFactory(name: String) = new ThreadFactory {
    private[this] val threadCount = new AtomicLong()
    override def newThread(r: Runnable) = {
      val thread = new Thread(r)
      thread.setName(name + "-" + threadCount.incrementAndGet())
      thread.setDaemon(true)
      thread
    }
  }

  /** Creates a new fixed-size thread pool with `threads` daemon threads */
  def newFixedThreadPool(name: String, threads: Int) = {
    Executors.newFixedThreadPool(threads, newDaemonThreadFactory(name))
  }

  /** Creates a new cached thread pool that creates new threads as needed, but will reuse previously constructed threads
   *   when they are available.
  */
  def newCachedThreadPool(name: String) = {
    Executors.newCachedThreadPool(newDaemonThreadFactory(name))
  }

  /** Creates a Skiis Context with a new fixed-size underlying thread pool of `parallelism` threads,
   *  with optional `queue` and `batch` configuration (both of which default to `1` unless provided).
   *
   *  @see Skiis.Context
   */
  def newContext(name: String, parallelism: Int, queue: Int = 1, batch: Int = 1) = {
    val _parallelism = parallelism
    val _queue = queue
    val _batch = batch
    new Context {
      override final val parallelism = _parallelism
      override final val queue = _queue
      override final val batch = _batch
      override final val shutdownExecutor = true
      override final lazy val executor = newFixedThreadPool(name, parallelism)
    }
  }

  private[Skiis] val _empty = new Skiis[Nothing] {
    override def next() = None
    override def take(n: Int) = Seq.empty
  }

  private[skiis2] trait MapOp[T, U] extends Skiis[U] {
    /** Previous Skiis collection in the chain; contains elements being consumed
     *  and to which the `f` operation is being applied.
     */
    protected var previous: Skiis[T]

    /** Stream-fusion of all user-defined processing being applied to `previous` elements.
     *
     *  This is a `var` because in the case of stream fusion, the function may
     *  be composed other function(s) and the result substituted for the original
     *  function.
     */
    protected var f: T => U

    /** Sequence function `f2` to this map operation */
    private[skiis2] def compose[V](f2: U => V): MapOp[T, V] = {
      val updated = this.asInstanceOf[MapOp[T, V]]
      updated.f = f andThen f2
      updated
    }

    override def next() = previous.next() map f
  }

  private[skiis2] trait FlatMapOp[U] extends Skiis[U] { self =>
    /** Previous Skiis collection in the chain; contains elements being consumed
     *  and to which the `enqueue` operation is being applied.
     */
    protected val previous: Skiis[_]

    /** Stream-fusion of all user-defined processing being applied to `previous` elements.
     *
     *  This is a `var` because in the case of stream fusion, the function may
     *  be composed other function(s) and the result substituted for the original
     *  function.
     */
    protected var applyAndRunContinuation: ApplyContinuation[_, U]

    /**
     *  An intermediate queue where elements are stored after processing and
     *  waiting to be consumed by this collection's own public methods.
     *
     *  A queue is required because `enqueue` may produce several elements at
     *  a time (e.g. if the upstream operation is a flatMap).
     */
    private[this] final val queue = new ArrayBuffer[U]()

    /** Lock for the unsynchronized `queue` */
    private[this] val lock = new ReentrantLock()

    /** Number of outstanding (concurrent) consumers, synchronized by `lock` */
    private[this] var consumers = 0

    /** Signaled when a consumer is done consuming.  */
    private[this] val doneConsuming = new Condition(lock)

    /** Set to `true` when the `previous` collection has no more elements. */
    private[this] var noMore = false

    /** Sequence a new operation to the existing operations */
    private[skiis2] def compose[T2, U2](f: Continuation[U] => ApplyContinuation[T2, U2]) = {
      val updated = self.asInstanceOf[FlatMapOp[U2]]
      updated.applyAndRunContinuation = f(universal(applyAndRunContinuation))
      updated
    }

    override def next(): Option[U] = {
      while (true) {
        lock.lock()
        try {
          if (queue.size > 0) return Some(queue.remove(0))
          if (noMore && consumers == 0) return None
          consumers += 1
        } finally lock.unlock()

        val next = previous.next()
        if (next == null || next.isEmpty) {
          lock.lock()
          try {
            consumers -= 1
            noMore = true
            if (consumers == 0) {
              doneConsuming.signalAll()
              None
            } else {
              doneConsuming.await()
            }
          } finally lock.unlock()
        } else {
          universal(applyAndRunContinuation).apply(next.get, enqueue)
          lock.lock()
          try {
            consumers -= 1
            doneConsuming.signalAll()
          } finally lock.unlock()
        }
      }
      sys.error("unreachable")
    }

    private[this] final def enqueue(u: U) {
      lock.lock()
      try queue += u
      finally lock.unlock()
    }
  }

  /** Construct Skiis[T] collection from an Iterator[T] */
  def apply[T](iter: Iterator[T]) = new Skiis[T] {
    override def next = iter.synchronized {
      if (iter.hasNext) Some(iter.next) else None
    }
    override def take(n: Int) = iter.synchronized { super.take(n) }
  }

  /** Construct Skiis[T] collection from an Iterable[T] */
  def apply[T](s: Iterable[T]): Skiis[T] = apply(s.iterator)

  /** Convenience construction for literal values */
  def apply[T](t: T, ts: T*): Skiis[T] = apply(Iterator(t) ++ ts.toIterator)

  def singleton[T](t: T): Skiis[T] = Skiis(Iterator(t))

  def empty[T]: Skiis[T] = _empty

  /** Merge / interleave several Skiis[T].
   *
   *  e.g. Skiis.merge(Skiis(1,2,3), Skiis(4,5), Skiis(6,7,8,9)) =>
   *           Skiis(1,4,6,2,5,7,3,8,9)
   *
   *  Element order in the resulting Skiis[T] is non-deterministic under
   *  concurrency but implementation attempts to pull elements from each
   *  underlying Skiis[T] fairly.
   */
  def merge[T](skiis: Skiis[T]*): Skiis[T] = new Skiis[T] {
    private[this] val ring = new ConcurrentRing(skiis)

    @tailrec override def next(): Option[T] = {
      val skii = ring.next()
      if (skii == None) return None

      val n = skii.get.next()
      if (n.isDefined) {
        n
      } else {
        ring.remove(skii.get)
        next()
      }
    }
  }

  /** Runs some computation `f` in a new (daemon) thread and return the thread */
  def async[T](f: => T): Future[T] = {
    val p = Promise[T]()
    val runnable = new Runnable {
      override def run() = {
        try {
          p.success(f)
        } catch { case t: Throwable =>
          p.failure(t)
        }
      }
    }
    cachedThreadPool.execute(runnable)
    p.future
  }

  /** Runs some computation `f` in a new (daemon) thread and return the thread */
  def async[T](name: String)(f: => T): Thread = {
    val t = new Thread(new Runnable() { override def run() { f } }, name)
    t.setDaemon(true)
    t.start()
    t
  }

  /** A Skiis[T] collection backed by a LinkedBlockingQueue[T]
   *  that allows "pushing" elements to consumers.
   */
  final class Queue[T](val size: Int) extends Skiis[T] {
    private[this] val queue = new LinkedBlockingQueue[T](size)
    private[this] var closed = false
    private[this] var closedImmediately = false
    private[this] var exception: Throwable = null
    private[this] var cancelled = false
    private[this] val lock = new ReentrantLock()
    private[this] val empty = lock.newCondition()
    private[this] val full = lock.newCondition()

    def cancel() {
      lock.lock()
      try {
        cancelled = true
        empty.signalAll()
        full.signalAll()
      } finally {
        lock.unlock()
      }
    }

    def reportException(t: Throwable) {
      lock.lock()
      try {
        if (exception == null) exception = t
        empty.signalAll()
        full.signalAll()
      } finally {
        lock.unlock()
      }
    }

    protected def bailOutIfNecessary() {
      lock.lock()
      try {
        if (exception != null) throw exception
        if (cancelled) throw new CancellationException("Queue processing was cancelled")
      } finally {
        lock.unlock()
      }
    }

    def +=(t: T) {
      lock.lock()
      try {
        while (!closedImmediately && !queue.offer(t)) {
          bailOutIfNecessary()
          full.await()
        }
        empty.signalAll()
      } finally {
        lock.unlock()
      }
    }

    def ++=(ts: Seq[T]) {
      lock.lock()
      try {
        for (t <- ts) {
          while (!closedImmediately && !queue.offer(t)) {
            bailOutIfNecessary()
            full.await()
          }
          empty.signalAll()
        }
      } finally {
        lock.unlock()
      }
    }

    def close(immediately: Boolean = false) {
      lock.lock()
      try {
        if (immediately) {
          closedImmediately = true
          queue.clear()
        } else {
          closed = true
        }
        full.signalAll()
        empty.signalAll()
      } finally {
        lock.unlock()
      }
    }

    override def next(): Option[T] = {
      lock.lock()
      try {
        while (!closedImmediately) {
          bailOutIfNecessary()
          val n = queue.poll()
          if (n != null) {
            full.signalAll()
            return Some(n)
          } else {
            if (closed) return None
            empty.await()
          }
        }
        None
      } finally {
        lock.unlock()
      }
    }

    override def take(n: Int): Seq[T] = {
      val result = new ArrayBuffer[T](n)
      take(n, result)
      result
    }

    override def take[TT >: T](n: Int, buffer: ArrayBuffer[TT]): Unit = {
      buffer.clear()
      lock.lock()
      try {
        while (buffer.size < n && !closedImmediately) {
          bailOutIfNecessary()
          val n = queue.poll()
          if (n != null) {
            buffer += n
            full.signalAll()
          } else {
            if (closed) return
            empty.await()
          }
        }
      } finally {
        lock.unlock()
      }
    }
  }

  @Experimental
  trait Control {
    def cancel(): Unit
    def isCancelled: Boolean
    def isDone: Boolean
  }

  trait Context {
    /** Maximum number of outstandig workers submitted to executor */
    val parallelism: Int

    /** Number of elements to be queued (work-in-progress) until workers temporarily stop processing */
    val queue: Int

    /** Number of elements handled by each worker before worker is re-submitted to executor.
     *  This is a tradeoff between processing efficiency and sharing the executor with other clients.
     */
    val batch: Int

    /** Underlying executor service, e.g., FixedThreadPool, ForkJoin, ... */
    val executor: ExecutorService

    val shutdownExecutor: Boolean

    /** Submit some computation `f` into the context's executor. */
    def submit[T](f: => T) = {
      executor.submit(new Runnable() { override def run() { f } })
    }

    def shutdown(now: Boolean = false): Unit = {
      if (shutdownExecutor) {
        if (now) executor.shutdownNow() else executor.shutdown()
      }
    }

    override def toString = {
      "%s(executor=%s, parallelism=%d, queue=%d, batch=%d)" format (getClass.getSimpleName, executor, parallelism, queue, batch)
    }

    def copy(parallelism: Int = this.parallelism, queue: Int = this.queue, batch: Int = this.batch, executor: ExecutorService = this.executor, shutdownExecutor: Boolean = false): Context = {
      val _parallelism = parallelism
      val _queue = queue
      val _batch = batch
      val _executor = executor
      val _shutdownExecutor = shutdownExecutor
      new Context {
        override final val parallelism = _parallelism
        override final val queue = _queue
        override final val batch = _batch
        override final val executor = _executor
        override final val shutdownExecutor = _shutdownExecutor
      }
    }
  }

  /** A default context provided for convenience.
   *
   *  This context is not meant to be used in production applications.
   *
   *  It should only be useful to lazy programmers who want to conveniently ignore
   *  best practices until their day of reckoning or when Murphy's law kicks in,
   *  whichever comes first.
   */
  object DefaultContext extends Context {
    override final val parallelism = (Runtime.getRuntime.availableProcessors + 1) * 10
    override final val queue = parallelism
    override final val batch = 1
    override final val shutdownExecutor = true
    override final lazy val executor = newCachedThreadPool(getClass.getName)
  }

  /** A deterministic context with a single thread and no parallelism.
   *
   *  Intended for use in test cases to reduce variability.
   *
   *  Determinism is only guaranteed if there are no other contexts or thread pools
   *  in use. The use of any kind of asynchronous execution implies some level of
   *  concurrency and variability, notably if multiple deterministic contexts
   *  are used in conjunction.
   *
   *  A different deterministic context should be used for every parallel operation
   *  (stage) in a Skiis pipeline to prevent possible starvation/deadlocks.
   */
  class DeterministicContext extends Context {
    override final val parallelism = 1
    override final val queue = 1
    override final val batch = 1
    override final val shutdownExecutor = true
    override final lazy val executor = newFixedThreadPool(getClass.getName, threads = 1)
  }

  class Experimental extends scala.annotation.Annotation
}
