package skiis

import java.util.concurrent._
import java.util.concurrent.atomic._
import java.util.concurrent.locks._

import scala.annotation.tailrec
import scala.annotation.unchecked.uncheckedVariance
import scala.collection.mutable.ArrayBuffer

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
 *  implicit executor and desired level of parallelism.  See Skiis.Context.
 */
trait Skiis[+T] extends { self =>
  import Skiis._

  /** Return the next element of this collection. */
  def next(): Option[T]

  /** Return the next `n` elements of this collection. */
  def take(n: Int): Seq[T] = {
    val buf = new ArrayBuffer[T](n)
    var i = 0
    while (i < n) {
      val n = next()
      if (n.isEmpty) {
        return buf
      }
      buf += n.get
      i += 1
    }
    buf
  }

  /** Transform elements of this collection using `f` and return a new collection. */
  def map[U](f: T => U): Skiis[U] = {
    val captureF = f
    self match {
      case map: MapOp[T @unchecked]  =>
        self.asInstanceOf[MapOp[U]].f = map.f andThen captureF // fusion
        self.asInstanceOf[Skiis[U]]

      case map: FlatMapOp[T @unchecked]  =>
        val previous = map.enqueue.asInstanceOf[(Any, T => Unit) => Unit]
        val enqueue = (x: Any, push: U => Unit) => previous(x, (t: T) => push(captureF(t))) // possibly fusion
        self.asInstanceOf[FlatMapOp[U]].enqueue = enqueue // fusion
        self.asInstanceOf[Skiis[U]]

      case _ =>
        new Skiis[U] with MapOp[U] {
          @volatile override var f: _ => U = captureF
          override def next() = self.next() map f.asInstanceOf[T => U]
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
    val capturedF = f

    self match {
      case map: FlatMapOp[T @unchecked] =>
        val previous = map.enqueue.asInstanceOf[(Any, T => Unit) => Unit]
        val enqueue = (x: Any, push: U => Unit) => previous(x, capturedF(_) foreach push) // possibly fusion
        self.asInstanceOf[FlatMapOp[U]].enqueue = enqueue // fusion
        self.asInstanceOf[Skiis[U]]

      case _ =>
        new Skiis[U] with FlatMapOp[U] {
          @volatile override var enqueue = (x: Any, push: U => Unit) => capturedF(x.asInstanceOf[T]) foreach push
          override val previous: Skiis[T] = self
        }
    }
  }

  /** Selects all elements of this collection which satisfy a predicate. */
  def filter(f: T => Boolean): Skiis[T] = withFilter(f)

  /** Selects all elements of this collection which satisfy a predicate. */
  def withFilter(f: T => Boolean): Skiis[T] = {
    self match {
      case map: FlatMapOp[T @unchecked] =>
        val previous = map.enqueue.asInstanceOf[(Any, T => Unit) => Unit]
        val enqueue = (x: Any, push: T => Unit) => previous(x, t => if (f(t)) push(t)) // possibly fusion
        self.asInstanceOf[FlatMapOp[T]].enqueue = enqueue // fusion
        self.asInstanceOf[Skiis[T]]

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
      case map: FlatMapOp[T @unchecked] =>
        val previous = map.enqueue.asInstanceOf[(Any, T => Unit) => Unit]
        val enqueue = (x: Any, push: U => Unit) => previous(x, t => if (f.isDefinedAt(t)) push(f(t))) // possibly fusion
        self.asInstanceOf[FlatMapOp[U]].enqueue = enqueue // fusion
        self.asInstanceOf[Skiis[U]]

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

  /** "Pull" all values from the collection, forcing evaluation of previous lazy computation. */
  def pull(): Unit = {
    foreach { _ => () }
  }

  def parPull()(implicit context: Context) {
    parForeachAsync { _ => () }.result // block for result
  }

  /** Applies a function `f` in parallel to all elements of this collection */
  def parForeach(f: T => Unit)(implicit context: Context) {
    parForeachAsync(f).result // block for result
  }

  /** Applies a function `f` in parallel to all elements of this collection */
  def parForeachAsync(f: T => Unit)(implicit context: Context): Control with Result[Unit] = {
    val job = new Job[Unit]() with Result[Unit] {
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
  def parMap[U](f: T => U)(implicit context: Context): Skiis[U] = {
    val job = new Job[U]() with Queuing[U] {
      override def process(t: T) = enqueue(f(t))
    }
    job.start()
    job
  }

  /** Transform elements of this collection in parallel with the function `f`
   *  producing zero-or-more  outputs per input element and return a new collection
   *  concatenating all the outputs.
   */
  def parFlatMap[U](f: T => Seq[U])(implicit context: Context): Skiis[U] = {
    val job = new Job[U]() with Queuing[U] {
      override def process(t: T) =  enqueue(f(t))
    }
    job.start()
    job
  }

  /** Selects (in parallel) all elements of this collection which satisfy a predicate. */
  def parFilter(f: T => Boolean)(implicit context: Context): Skiis[T] = {
    val job = new Job[T]() with Queuing[T] {
      override def process(t: T) =  { if (f(t)) enqueue(t) }
    }
    job.start()
    job
  }

  /** Selects (in parallel) all elements of this collection which do not satisfy a predicate. */
  def parFilterNot(f: T => Boolean)(implicit context: Context): Skiis[T] = {
    val job = new Job[T]() with Queuing[T] {
      override def process(t: T) =  { if (!f(t)) enqueue(t) }
    }
    job.start()
    job
  }

  /** Filter and transform elements of this collection (in parallel) using the partial function `f` */
  def parCollect[U](f: PartialFunction[T, U])(implicit context: Context): Skiis[U] = {
    val job = new Job[U]() with Queuing[U] {
      override def process(t: T) =  { if (f.isDefinedAt(t)) enqueue(f(t)) }
    }
    job.start()
    job
  }

  /* ALEX: Commented out due to the serial nature of the operation.
  def parFold[U](initial: U)(f: (T, U) => U)(implicit context: Context): U = {
    val job = new Job[U]() with Result[U] {
      private val acc = new AtomicReference[U](initial)

      private val completed = new Condition(lock)
      private val available = new Condition(lock)

      override def process(t: T) {
        var done = false
        while (!done) {
          bailOutIfNecessary()

          lock.lock()
          try {
            val current = acc.getAndSet(null.asInstanceOf[U])
            if (current == null) {
              available.await()
            } else {
              val next = try { lock.unlock(); f(t, current) } finally { lock.lock() }
              acc.set(next)
              available.signal()
              done = true
            }
          } finally {
            lock.unlock()
          }
        }
      }

      override def notifyExceptionOrCancelled() = { completed.signalAll(); available.signalAll() }
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
        } finally {
          lock.unlock()
        }
        acc.get
      }
    }
    job.start()
    job.result
  }
  */

  def parReduce[TT >: T](f: (TT, T) => TT)(implicit context: Context): TT = {
    val job = new Job[T]() with Result[TT] {
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

  /** Job holds completion status and computation output */
  private[Skiis] abstract class Job[U](implicit val context: Context) extends Control { job =>
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
      t.printStackTrace()
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
      def run: Unit = {
        try {
          bailOutIfNecessary()
          val next = Skiis.this.take(batch)
          if (next.size < batch) {
            lock.lock()
            try {
              noMore = true
              notifyPossiblyNoMore()
            } finally {
              lock.unlock()
            }
          }
          val iter = next.iterator
          while (iter.hasNext) {
            process(iter.next)
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

    protected final def enqueue(output: U) = {
      results.put(Some(output))
      available.signal()
    }

    protected final def enqueue(outputs: Seq[U]) = {
      outputs foreach { output => results.put(Some(output)) }
      available.signalAll()
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
      var deadline = if (timeout >= 0) {
        System.currentTimeMillis + unit.toMillis(timeout)
      } else {
        Long.MaxValue
      }
     lock.lock()
     try {
        while (workersOutstanding > 0 || !noMore || results.size > 0) {
          bailOutIfNecessary()
          val next = results.poll()
          if (next != null) {
            startWorkers()
            return next
          }
          startWorkers()
          available.await(deadline - System.currentTimeMillis)
        }
      } finally {
        lock.unlock()
      }
      return None
    }
  }

  private[Skiis] trait Result[U] {
    /** Block for result, may throw exception if underlying computation failed. */
    def result: U
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

    def signal() = {
      lock.lock()
      try {
        condition.signal()
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

  /** ThreadFactory that creates named daemon threads */
  private [skiis] def daemonThreadFactory(name: String) = new ThreadFactory {
    private[this] val threadCount = new AtomicLong()
    override def newThread(r: Runnable) = {
      val thread = new Thread(r)
      thread.setName(name + "-" + threadCount.incrementAndGet())
      thread.setDaemon(true)
      thread
    }
  }

  private[Skiis] val _empty = new Skiis[Nothing] {
    override def next() = None
    override def take(n: Int) = Seq.empty
  }

  private[skiis] trait MapOp[T] extends Skiis[T] {
    private[skiis] var f: _ => T
  }

  private[skiis] trait FlatMapOp[U] extends Skiis[U] { self =>
    private[skiis] val previous: Skiis[Any]

    private[skiis] var enqueue: (Any, U => Unit) => Unit

    private[this] final val queue = new ArrayBuffer[U]()

    override def next(): Option[U] = synchronized {
      @tailrec def next0(): Option[U] = {
        if (queue.size > 0) {
          return Some(queue.remove(0))
        }
        val next = previous.next()
        if (next == null || next.isEmpty) {
          None
        } else {
          enqueue(next.get, queue += _)
          next0()
        }
      }
      next0()
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

  def async[T](name: String)(f: => T): Thread = {
    val t = new Thread(new Runnable() { override def run() { f } }, name)
    t.setDaemon(true)
    t.start()
    t
  }

  def submit[T](f: => T)(c: Context) = {
    c.executor.submit(new Runnable() { override def run() { f } })
  }

  /** A Skiis[T] collection backed by a LinkedBlockingQueue[T]
   *  that allows "pushing" elements to consumers.
   */
  final class Queue[T](val size: Int) extends Skiis[T] {
    private[this] val queue = new LinkedBlockingQueue[T](size)
    private[this] var closed = false
    private[this] val lock = new ReentrantLock()
    private[this] val empty = lock.newCondition()
    private[this] val full = lock.newCondition()

    def +=(t: T) {
      lock.lock()
      try {
        while (!queue.offer(t)) {
          full.await()
        }
        empty.signal()
      } finally {
        lock.unlock()
      }
    }

    def ++=(ts: Seq[T]) {
      lock.lock()
      try {
        for (t <- ts) {
          while (!queue.offer(t)) {
            full.await()
          }
        }
        empty.signalAll()
      } finally {
        lock.unlock()
      }
    }

    def close() {
      lock.lock()
      try {
        closed = true
        empty.signalAll()
      } finally {
        lock.unlock()
      }
    }

    override def next(): Option[T] = {
      lock.lock()
      try {
        while (true) {
          if (closed) return None
          val n = queue.poll()
          if (n != null) {
            full.signal()
            return Some(n)
          }
          empty.await()
        }
        sys.error("unreachable")
      } finally {
        lock.unlock()
      }
    }

    override def take(n: Int): Seq[T] = {
      val result = new ArrayBuffer[T](n)
      lock.lock()
      try {
        while (result.size < n && (queue.size > 0 || !closed)) {
          val isFull = (queue.size == size)
          val n = queue.poll()
          if (n != null) {
            result += n
            if (isFull) full.signal()
          } else {
            empty.await()
          }
        }
        full.signalAll()
        return result

        sys.error("unreachable")
      } finally {
        lock.unlock()
      }
    }
  }

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

    override def toString = {
      "%s(executor=%s, parallelism=%d, queue=%d, batch=%d)" format (getClass.getSimpleName, executor, parallelism, queue, batch)
    }
  }

  object DefaultContext extends Context {
    override final val parallelism = Runtime.getRuntime.availableProcessors + 1
    override final val queue = 100
    override final val batch = 10
    override final lazy val executor = Executors.newFixedThreadPool(parallelism, daemonThreadFactory(getClass.getName))
  }

  object DeterministicContext extends Context {
    override final val parallelism = 1
    override final val queue = 1
    override final val batch = 1
    override final lazy val executor = Executors.newFixedThreadPool(1, daemonThreadFactory(getClass.getName))
  }

}
