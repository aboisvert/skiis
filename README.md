Skiis: Streaming + Parallel collection for Scala
================================================

Think "Parallel Skiis"

Skiis are streaming / iterator-like collections that support both
"stream fusion" and parallel operations.

Skiis address different needs (and some deficiencies depending on your point of
view) than the standard Scala {serial, parallel} collections,

* All operations from `Skiis[T]` => `Skiis[U]` are lazy -- processing
  happens only on as-needed basis.  This applies to both serial (e.g. `map`) and
  parallel operations (e.g., `parMap`)

* Serial/parallel processing are explicit by name (e.g., `map` vs `parMap`) --
  no guessing how each operation processes the data.

* Streaming-oriented framework is memory-safe -- won't cause bad surprises
  since none of the operations will suddenly materialize a huge data collection
  in memory or lead to memory retention as is sometimes the case when handling
  Streams (i.e., keeping reference to the `head`).  Skiis have fewer operations
  than Scala collections but those offered are amenable to stream-  and/or
  parallel-processing.  You can easily and explicitly convert `Skiis[T]` to an
  `Iterator[T]` if you want to use Scala collections methods -- caveat emptor!

* You can mix serial and parallel processing operations together to tailor the
  level of parallelism at each stage of your processing pipeline for SEDA-style
  pull-based streaming architecture.

* Pluggable execution context -- you can easily plug your own `Executor`
  (thread pool) and/or use a different `Executor` for different parallel operations.
  (Note this deficiency only applies to Scala 2.9.x since 2.10.0
  introduced pluggable contexts).

* Beyond pluging-in your own `Executor`, you can also control the level of
  1) parallelism -- number of workers simultaneously submitted to Executor,
  2) queuing -- number of work-in-progress elements between parallel operations
  to balance memory usage against parallelism and 3) batch size -- to balance
  efficiency/contention against sharing your executor(s)/thread-pool(s)
  with other tasks (whether they use `Skiis` or not).

* `Skiis[T]` exposes a `Control` trait that supports cancellation.
  (This feature is currently experimental.)

See `CHANGELOG.md` for evolution details.

### Performance ###

Skiis' performance is generally comparable to Scala Parallel Collections --
sometimes better, sometimes worse. It depends on your workload (types of
operations), the thread pool you use, the allowable queue depth, worker batch
size, and so on.

I have not yet tested Skiis on machines with > 16 CPU cores.

### Examples ###

Launch your Scala REPL,

    # launch Scala REPL
    buildr shell

and you can then interactively try the Skiis[T] collections,

    Welcome to Scala version 2.9.1.final (Java HotSpot(TM) 64-Bit Server VM, Java 1.7.0_04).
    Type in expressions to have them evaluated.
    Type :help for more information.

    scala> import skiis2.Skiis

    // Define a timing function
    scala> def ptime[A](f: => A) = {
         |   val start = System.nanoTime
         |   val result = f
         |   printf("Elapsed: %.3f sec\n", (System.nanoTime - start) * 1e-9)
         |   result
         | }
    ptime: [A](f: => A)A

    // Stream fusion

    scala> ptime {
         |   Skiis(1 to 20)
         |     .map (_ * 3)
         |     .filter (_ % 2 == 0)
         |     .to[List]
         | }
    Elapsed: 0.001 sec
    res1: List[Int] = List(6, 12, 18, 24, 30, 36, 42, 48, 54, 60)

    // Parallel operations
    // (note unordered nature of the output)

    scala> Skiis(1 to 20)
             .parMap (_ * 3) (DefaultContext)
             .parFilter (_ % 2 == 0) (DefaultContext)
             .to[List]
    res3: List[Int] = List(6, 12, 18, 24, 30, 36, 42, 48, 54, 60)

    // You can mix & match non-parallel and parallel operations
    // Here the last parallel operation (reduce) "pulls" elements from
    // previous operations in parallel.

    scala> ptime {
     |   Skiis(1 to 20)
     |     .parMap (_ * 3) (DefaultContext)
     |     .parFilter (_ % 2 == 0) (DefaultContext)
     |    .to[List]
     | }
     Elapsed: 0.003 sec
     res4: List[Int] = List(6, 12, 18, 24, 30, 36, 42, 48, 54, 60)


    // compared to Scala parallel collection (we're in the same ballpark)

    scala> ptime {
         | Skiis(1 to 100000)
         |   .map (_ * 3)
         |   .filter (_ % 21 == 0)
         |   .parReduce (_ + _)(DefaultContext)
         | }
    Elapsed: 0.064 sec
    res5: Int = 2142792855

    // compared to Scala "serial" collections
    // (all parallel collections have some overhead)

    scala> ptime {
         | (1 to 100000)
         |   .map (_ * 3)
         |   .filter (_ % 21 == 0)
         |   .reduce (_ + _)
         | }
    Elapsed: 0.021 sec
    res71: Int = 2142792855

### Caveats ###

* Similarly to `Iterator`, Skiis do not implement content-based equals() or hashCode().
  If you want to compare Skiis, first convert them to a specific collection that supports
  equality, such as `Seq`.

### Building ###

You need Apache Buildr 1.4.x or higher.

    # compile, test and package .jars
    buildr package

### Target platform ###

* Scala 2.10+
* JVM 1.6+

### License ###

Skiis is is licensed under the terms of the Apache Software License v2.0.
<http://www.apache.org/licenses/LICENSE-2.0.html>

