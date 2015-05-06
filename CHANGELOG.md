
2.0.0 (2015-05-01)

Version 2 introduces many backward-incompatible changes and the package name
was changed from "skiis" to "skiis2" to allow both v1/v2 to coexist in
applications.

* The Skiis.Context is now explicit to all parallel operations.

  This is to avoid the case where the same context is used in several
  parallel operations without realizing, which can lead to issues of
  starvation and deadlock.

  The recommended practice is to use different Skiis.Context for all
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
  standard Scala collections.  It is used to convert a Skiis[T] to an
  arbitrary `Collection[T]`, e.g.,

    records.to[IndexedSeq]
    records.to[Set]

* The `parForeachAsync()` method and `Control` have been marked as
  `@Experimental` to set expectations and prevent premature production use.

* A new method `Skiis.newCachedThreadPool(name)` was introduced for convenience.

* `Skiis.Context` has a new method 'shutdown(now: Boolean)' to conditionally
  shutdown its executor, and a corresponding abstract field `shutdownExecutor`
  was added.

* A new convenience method `Context.submit(f: => T)` was added to easily submit
  asynchronous jobs into a Context's thread pool.

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

0.1.2 (2015-04-30)

 * Updated: `Queue.close()` method now accepts an `immediately` parameter
   which determines if the any remaining elements in the queue
   should be consumed by following stages.

0.1.1 (2013-01-26)
 * Added: `Skiis.Queue[T]` collection backed by a `LinkedBlockingQueue[T]`
          that allows "pushing" elements to consumers.

 * Added: `Skiis.zip()`, `Skiis.zipWithIndex()`, `Skiis.++()`

0.1.0 (2013-01-14)
 * First alpha





