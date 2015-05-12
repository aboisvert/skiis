
# Version 2.0.0 (2015-05-01)

Version 2 introduces several backward-incompatible changes and as a result
the package name was changed from `skiis` to `skiis2` to allow both v1/v2 to
coexist in applications.

* The `Skiis.Context` is now explicit to all parallel operations.

  This is to avoid a common pitfall where the same context is used in several
  parallel operations without realizing it, which can lead to issues of
  starvation and deadlock.

  The recommended practice is to use different `Skiis.Context` for all
  parallel operations that are combined together, and of course tailoring
  queuing, batch and parallelism parameters to each stage.

* The existing `pull()` method was renamed to `discardAll()` to better
  reflect its semantic, which is to force evaluation of previous lazy
  computations and discard all resulting values.

* The `pull()` and `parPull()` methods were repurposed to return a `Skiis[T]`
  collection instead of returning `Unit`.   Both force the evaluation of
  previous lazy computation and place resulting values in a queue
  (of explicitly specified size) for consumption by the next stage.

* A new method `to[Collection]` was added which behaves the same as
  standard Scala collections.  It is used to convert a `Skiis[T]` to an
  arbitrary `Collection[T]`, e.g.,

        skiis.to[IndexedSeq]  =>  IndexedSeq[T]
        skiis.to[Set]         =>  Set[T]

* A new method `serialize()` can be used to force serialized (no parallelism,
  no concurrency) execution of any preceeding computation up to the preceeding
  stage.

* The `parForeachAsync()` method and `Control` trait have been marked as
  `@Experimental` to set expectations and prevent premature production use.

* A new method `Skiis.newCachedThreadPool(name)` was introduced for convenience,
  which, similarly to `newFixedThreadPool`, creates named daemond threads.

* `Skiis.Context` has a new method `shutdown(now: Boolean)` to conditionally
  shutdown its executor, and a corresponding abstract field `shutdownExecutor`
  was added.

* A new convenience method `Context.submit(f: => T)` was added to easily submit
  asynchronous jobs into a Context's thread pool.

* A new convenience method `Skiis.submit(f: => T)` was added to easily submit
  asynchronous jobs into Skiis' internal cached thread pool and returns 
  a Scala `Future[T]`.

* A new `copy()` method was added to `Skiis.Context` and behaves as a case-class
  copy constructor.

* The `DefaultContext` now uses a `CachedThreadPool` executor (to prevent
  accidental starvation/deadlocks if reused across multiple stages) and
  defaults to the following values:

        val parallelism = (Runtime.getRuntime.availableProcessors + 1) * 10
        val queue = parallelism
        val batch = 1
        val shutdownExecutor = true

  As the updated Scaladoc says, this context is not meant to be used in
  production applications.  It should only be useful to lazy programmers who
  want to conveniently ignore best practices until their day of reckoning or
  when Murphy's law kicks in, whichever comes first.

* The `DeterministicContext` object was converted into a `class` to help
  prevent reuse in multiple stages of a Skiis pipeline.   Its scaladoc was
  updated with warnings and disclaimers for the unsuspecting programmer.

* A new method `parFold()` provides the ability to fold a `Skiis[T]` in parallel
  by using as many folding values as `context.parallelism` indicates.

* A new method `parFoldMap()` provides a combination of parallel fold and flatMap.
  It encodes a common pattern where a `Skiis[T]` is processed concurrently in a
  stateful manner and yields another `Skiis[U]`.  Since the states may reference or
  carry resources, `parFoldMap(()` supports init/dispose blocks.

* New methods `parMapWithQueue()` and `parFoldWithQueue()` are optimized versions
  of `parFlatMap()` and `parFoldMap()` where the supplied functions (procedures)
  must directly produce to the result queue instead of yielding values.

* A new method `fanout(queues: Int, queueSize: Int): IndexedSeq[Queue[T]]` can
  be used to replicate a `Skiis[T]` and create a fan-out pattern by replicating each
  element in multiple fan-out queues.

* `Skiis.Queue[T]` now provides `cancel()` and `reportException()` methods to
  cancel or fail dependent computations.

For changes prior to V2, please see `CHANGELOG-v1.md`.


