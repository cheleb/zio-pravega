# ZIO

ZIO is an effect system for Scala that makes it easy and fun to write concurrent programs that never leak resources or deadlock. ZIO's data types and principled, composable concurrency primitives make it possible to build large, scalable, highly performant systems that are easily testable and debuggable.

ZIO is powered by [Fiber](https://zio.dev/docs/datatypes/datatypes_fiber) data type, which is an implementation of [Fiber, a lightweight thread of execution](https://en.wikipedia.org/wiki/Fiber_(computer_science)).

ZIO is a [type-safe](https://en.wikipedia.org/wiki/Type_safety) alternative to [monads](https://en.wikipedia.org/wiki/Monad_(functional_programming)) such as [Future](https://docs.scala-lang.org/overviews/core/futures.html) or [IO](https://typelevel.org/cats-effect/datatypes/io.html). Unlike monads, ZIO is not a [type constructor](https://en.wikipedia.org/wiki/Type_constructor), and this makes it possible to use ZIO in a wider range of scenarios than monads, such as [higher-kinded types](https://en.wikipedia.org/wiki/Kind_(type_theory)).

It is a pure functional programming library for everyday programming.