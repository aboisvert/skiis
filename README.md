Skiis: Streaming + Parallel collection for Scala
================================================

Think "Parallel Skiis"

Skiis are streaming / iterator-like collections that support both
"stream fusion" and parallel operations.

Skiis address a few deficiencies in the standard Scala collections,

* All operations from Skiis[T] => Skiis[U] are non-strict -- processing happens
  only on as-needed basis.  This applies to both serial (e.g. `map`) and parallel operations (e.g., `parMap`)

* Serial/parallel processing are explicit by name (e.g., `map` vs `parMap`) --
  no guessing how each operation processes the data.

* Streaming-oriented framework is memory-safe -- won't cause bad suprises
  since none of the operations will suddenly materialize a huge data collections
  in memory or lead to "memory retention" as sometimes the case when handling
  Streams.  Skiis have fewer operations than Scala collections but those
  offered are amenable to stream-  and/or parallel-processing.
  (You can easily and explicitly convert Skiis[T] to an Iterator[T] if you want
  to use Scala collections methods -- caveat emptor!)

* You can mix lazy (stream-fusion) and parallel processing operations together
  to tailor the level of parallelism at each stage of your processing pipeline
  for SEDA-style pull-based streaming architecture.

* Pluggable execution context -- you can easily plug your own Executor
  (thread pool) -- although this only applies to Scala 2.9.x since 2.10.0
  introduced that feature.

* Beyond pluging-in your own Executor, you can also control the level of
  parallelism (number of workers submitted to Executor) and queuing (number of
  work-in-progress elements) of parallel operations to balance memory usage against parallelism.

* Skiis[T] exposes a Control trait that supports cancellation.

See CHANGELOG for evolution details.

### Performance ###

Skiis[T] are most likely slower than Scala Parallel Collections -- Skiis has
been designed with coarsed grained operations in mind.

NO CLAIM OF PERFORMANCE COMPARED TO STANDARD SCALA PARALLEL COLLECTIONS IS MADE.

### Examples ###

Launch your Scala REPL,

    # launch Scala REPL
    buildr shell

    Welcome to Scala version 2.9.1.final (Java HotSpot(TM) 64-Bit Server VM, Java 1.7.0_04).
    Type in expressions to have them evaluated.
    Type :help for more information.

    scala> import skiis.Skiis

    // Stream fusion

    scala> Skiis(1 to 10) map (_ * 2) filter (_ % 2 == 0)
    res1: skiis.Skiis[Int] = skiis.Skiis$$anon$11@456e5841

    scala> res1.toIterator.toList
    res4: List[Int] = List(4, 6, 8, 10, 12, 14, 16, 18, 20)

    // Parallel operations
    // (note unordered nature of the output)

    scala> implicit val context = Skiis.DefaultContext
    context: skiis.Skiis.DefaultContext.type = skiis.Skiis$DefaultContext$@49b7bb1f

    scala> Skiis(1 to 10) parMap (_ * 2) parFilter (_ % 2 == 0)
    res1: skiis.Skiis[Int] = skiis.Skiis$$anon$3@6a39f22c

    scala> res1.toIterator.toList
    res2: List[Int] = List(6, 12, 16, 18, 20, 4, 14, 2, 8, 10)

    // You can mix & match non-parallel and parallel operations
    // Here the last parallel operation (reduce) "pulls" elements from
    // previous operations in parallel.

    scala> Skiis(1 to 100000) map (_ * 2) filter (_ % 2 == 0) parReduce (_ + _)
    res1: Int = 1410165408
    (Elapsed 62ms)

    // compared to Scala parallel collection (we're in the same ballpark)

    scala> (1 to 100000).par map (_ * 2) filter (_ % 2 == 0) reduce (_ + _)
    res1: Int = 1410165408
    (Elapsed 54ms)

    // compared to Scala non-parallel collections
    // (all parallel collections have some overhead)

    scala> (1 to 100000).par map (_ * 2) filter (_ % 2 == 0) reduce (_ + _)
    res1: Int = 1410165408
    (Elapsed 27ms)


    def time[T](f: => T) = {
      val start = System.currentTimeMillis
      val result = f
      val stop = System.currentTimeMillis
      println("Elapsed: " + (stop-start) + "ms")
      result
    }

### Building ###

You need Apache Buildr 1.4.x or higher.

    # compile, test and package .jars
    buildr package


### Target platform ###

* Scala 2.8.0+
* JVM 1.5+

### License ###

Skiis is is licensed under the terms of the Apache Software License v2.0.
<http://www.apache.org/licenses/LICENSE-2.0.html>

