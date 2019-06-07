grimlock
========

[![Build Status](https://travis-ci.org/CommBank/grimlock.svg?branch=master)](https://travis-ci.org/CommBank/grimlock)

Overview
--------

The grimlock library can be used for performing data-science and machine learning related data preparation, aggregation, manipulation and querying tasks. It makes it easy to perform such tasks as:

* Normalisation/Standardisation/Bucketing of numeric variables;
* Binarisation of categorical variables;
* Creating indicator variables;
* Computing statistics in all dimensions;
* Generating data for a variety of machine learning tools;
* Partitioning and sampling of data;
* Derived features such as gradients and moving averages;
* Text analysis (tf-idf/LDA);
* Computing pairwise distances.

The library contains default implementations for many of the above tasks. It also has a number of useful properties:

* Supports wide variety of variable types;
* Is easily extensible;
* Can operate in multiple dimensions;
* Supports heterogeneous data;
* Can be used in the Scala/Scalding/Spark REPL;
* Supports basic as well as structured data types.

Getting Started
-------------

Simply add the following lines to your build file:

```
libraryDependencies += "au.com.cba.omnia" %% "grimlock-core" % "0.7.7"
resolvers += "commbank-releases" at "http://commbank.artifactoryonline.com/commbank/ext-releases-local"
```

For information on the API, see the [scaladoc](https://commbank.github.io/grimlock/latest/api/index.html). Links to detailed examples are included at the end of the Scalding and Spark sections below.

Concepts
--------

### Data Structures

The basic data structure in grimlock is a N-dimensional sparse __Matrix__. Each __Cell__ in a Matrix consists of a __Position__ and __Content__.

```
          Matrix
            ^ 1
            |
            | M
  Cell(Position, Content)
```

The Position is, essentially, a list of N coordinates (where each coordinate is stored in a __Value__). The Content consists of a __Type__ together with a Value. The Value contains the data (Double, Sting, Date, etc.) of the cell, while the Type defines the variable type of the data. Note, the Type is obtained from a __Schema__ which (optionally) defines the value's legal values.

```
   Position              Content
       ^ 1                  ^ 1
       |                    |
       | N           +------+------+
     Value           | 1           | 1
                    Type         Value
```

Lastly, a __Codec__ can be used to parse, write and compare the data types used in the values.

```
  Value
    ^ 1
    |
    | 1
  Codec
```

### Working with dimensions

Performing operations along any of the dimensions of the matrix is supported through a __Slice__. There are two realisations of Slice: __Along__ and __Over__. Both are constructed with one or more dimensions, specified as natural numbers (__shapeless.Nat__)  where \_0 indexes the first dimension. However they differ in how the dimension is interpreted. When using Over, all data in the matrix is grouped by the dimension(s) and operations, such as aggregation, are applied to the resulting groups. When using Along, the data is grouped by all dimensions *except* the dimension(s) used when constructing the Slice. The differences between Over and Along are graphically presented below for a three dimensional matrix. Note that in 2 dimensions, Along and Over are each other's inverse.

```
        Over(_1)          Along(_2)

     +----+------+      +-----------+
    /    /|     /|     /     _     /|
   /    / |    / |    /    /|_|   / |
  +----+------+  |   +-----------+  |
  |    |  |   |  |   |   /_/ /   |  |
  |    |  +   |  +   |   |_|/    |  +
  |    | /    | /    |           | /
  |    |/     |/     |           |/
  +----+------+      +-----------+
```

### Data Format

The basic data format used by grimlock (though others are supported) is a column-oriented data file (each row is a single cell, separated by a delimiter). The first N fields are the coordinates, optionally followed by the variable type and codec. If the variable type and codec are omitted from the data then they have to be provided by a __Dictionary__. The last field of each row is the value.

In the example below the first field is a coordinate identifying an instance, the second field is a coordinate identifying a feature. The third and fourth columns are the codec and variable type respectively. The last column has the actual value.

```
> head <path to>/grimlock/grimlock-examples/src/main/scala/commbank/grimlock/data/exampleInput.txt
iid:0064402|fid:B|string|nominal|H
iid:0064402|fid:E|long|continuous|219
iid:0064402|fid:H|string|nominal|C
iid:0066848|fid:A|long|continuous|371
iid:0066848|fid:B|string|nominal|H
iid:0066848|fid:C|long|continuous|259
iid:0066848|fid:D|string|nominal|F
iid:0066848|fid:E|long|continuous|830
iid:0066848|fid:F|string|nominal|G
iid:0066848|fid:H|string|nominal|B
...
```

If the type and codec were omitted then the data would look as follows:

```
iid:0064402|fid:B|H
iid:0064402|fid:E|219
iid:0064402|fid:H|C
iid:0066848|fid:A|371
iid:0066848|fid:B|H
iid:0066848|fid:C|259
iid:0066848|fid:D|F
iid:0066848|fid:E|830
iid:0066848|fid:F|G
iid:0066848|fid:H|B
...
```

An external dictionary will then have to be provided to correctly decode and validate the values:

```
fid:A|long|continuous
fid:B|string|nominal
fid:C|long|continuous
fid:D|string|nominal
fid:E|long|continuous
fid:F|string|nominal
fid:H|string|nominal
...
```

Usage - Scala
-----

### Setting up REPL

The examples below are executed in the Scala REPL. To use grimlock in the REPL follow the following steps:

1. Clone this repository.
2. Start REPL; `./sbt grimlock-core/console`.

After the last command, the console should appear as follows:

```
> ./sbt grimlock-core/console
...
[info] Starting scala interpreter...
[info]
Welcome to Scala version 2.11.8 (OpenJDK 64-Bit Server VM, Java 1.8.0_181).
Type in expressions for evaluation. Or try :help.

scala>
```

Note, for readability, the REPL info is suppressed from now on.

### Getting started

When at the Scala REPL console, the first step is to import grimlock's functionality (be sure to press ctrl-D after the last import statement):

```
> scala> :paste
// Entering paste mode (ctrl-D to finish)

import commbank.grimlock.framework._
import commbank.grimlock.framework.encoding._
import commbank.grimlock.framework.environment.implicits._
import commbank.grimlock.framework.position._

import commbank.grimlock.library.aggregate._

import commbank.grimlock.scala.environment._
import commbank.grimlock.scala.environment.implicits._

import shapeless.HNil
import shapeless.nat.{ _0, _1 }
```

Next, for convenience, set up grimlock's Context as an implicit:

```
scala> implicit val context = Context()
```

The next step is to read in data (be sure to change <path to> to the correct path to the grimlock repo):

```
scala> val (data, _) = context.loadText(
  "<path to>/grimlock/grimlock-examples/src/main/scala/commbank/grimlock/data/exampleInput.txt",
  Cell.shortStringParser(StringCodec :: StringCodec :: HNil, "|")
)
```

The returned `data` is a 2 dimensional matrix. To investigate it's content Scala's `foreach` command can be used in the REPL, use grimlock's `saveAsText` API for writing to disk:

```
scala> data.foreach(println)
Cell(Position(StringValue(iid:0064402,StringCodec) :: StringValue(fid:B,StringCodec) :: HNil),Content(NominalType,StringValue(H,StringCodec)))
Cell(Position(StringValue(iid:0064402,StringCodec) :: StringValue(fid:E,StringCodec) :: HNil),Content(ContinuousType,LongValue(219,LongCodec)))
Cell(Position(StringValue(iid:0064402,StringCodec) :: StringValue(fid:H,StringCodec) :: HNil),Content(NominalType,StringValue(C,StringCodec)))
Cell(Position(StringValue(iid:0066848,StringCodec) :: StringValue(fid:A,StringCodec) :: HNil),Content(ContinuousType,LongValue(371,LongCodec)))
Cell(Position(StringValue(iid:0066848,StringCodec) :: StringValue(fid:B,StringCodec) :: HNil),Content(NominalType,StringValue(H,StringCodec)))
Cell(Position(StringValue(iid:0066848,StringCodec) :: StringValue(fid:C,StringCodec) :: HNil),Content(ContinuousType,LongValue(259,LongCodec)))
Cell(Position(StringValue(iid:0066848,StringCodec) :: StringValue(fid:D,StringCodec) :: HNil),Content(NominalType,StringValue(F,StringCodec)))
Cell(Position(StringValue(iid:0066848,StringCodec) :: StringValue(fid:E,StringCodec) :: HNil),Content(ContinuousType,LongValue(830,LongCodec)))
Cell(Position(StringValue(iid:0066848,StringCodec) :: StringValue(fid:F,StringCodec) :: HNil),Content(NominalType,StringValue(G,StringCodec)))
Cell(Position(StringValue(iid:0066848,StringCodec) :: StringValue(fid:H,StringCodec) :: HNil),Content(NominalType,StringValue(B,StringCodec)))
...
```

The following shows a number of basic operations (get number of rows, get type of features, perform simple query):

```
scala> data.measure(_0).foreach(println)
Cell(Position(LongValue(0,LongCodec) :: HNil),Content(DiscreteType,LongValue(9,LongCodec)))

scala> data.types(Over(_1))(false).foreach(println)
Cell(Position(StringValue(fid:A,StringCodec) :: HNil),Content(NominalType,TypeValue(NumericType,TypeCodec)))
Cell(Position(StringValue(fid:B,StringCodec) :: HNil),Content(NominalType,TypeValue(CategoricalType,TypeCodec)))
Cell(Position(StringValue(fid:C,StringCodec) :: HNil),Content(NominalType,TypeValue(NumericType,TypeCodec)))
Cell(Position(StringValue(fid:D,StringCodec) :: HNil),Content(NominalType,TypeValue(CategoricalType,TypeCodec)))
Cell(Position(StringValue(fid:E,StringCodec) :: HNil),Content(NominalType,TypeValue(NumericType,TypeCodec)))
Cell(Position(StringValue(fid:F,StringCodec) :: HNil),Content(NominalType,TypeValue(CategoricalType,TypeCodec)))
Cell(Position(StringValue(fid:G,StringCodec) :: HNil),Content(NominalType,TypeValue(NumericType,TypeCodec)))
Cell(Position(StringValue(fid:H,StringCodec) :: HNil),Content(NominalType,TypeValue(CategoricalType,TypeCodec)))

scala> data.which(cell => (cell.content.value gtr 995) || (cell.content.value equ "F")).foreach(println)
Position(StringValue(iid:0066848,StringCodec) :: StringValue(fid:D,StringCodec) :: HNil)
Position(StringValue(iid:0216406,StringCodec) :: StringValue(fid:E,StringCodec) :: HNil)
Position(StringValue(iid:0444510,StringCodec) :: StringValue(fid:D,StringCodec) :: HNil)
```

Now for something a little more interesting. Let's compute the number of features for each instance and then compute the moments of the distribution of counts:

```
scala> val counts = data.summarise(Over(_0))(Counts())

scala> counts.foreach(println)
Cell(Position(StringValue(iid:0064402,StringCodec) :: HNil),Content(DiscreteType,LongValue(3,LongCodec)))
Cell(Position(StringValue(iid:0066848,StringCodec) :: HNil),Content(DiscreteType,LongValue(7,LongCodec)))
Cell(Position(StringValue(iid:0216406,StringCodec) :: HNil),Content(DiscreteType,LongValue(5,LongCodec)))
Cell(Position(StringValue(iid:0221707,StringCodec) :: HNil),Content(DiscreteType,LongValue(4,LongCodec)))
Cell(Position(StringValue(iid:0262443,StringCodec) :: HNil),Content(DiscreteType,LongValue(2,LongCodec)))
Cell(Position(StringValue(iid:0364354,StringCodec) :: HNil),Content(DiscreteType,LongValue(5,LongCodec)))
Cell(Position(StringValue(iid:0375226,StringCodec) :: HNil),Content(DiscreteType,LongValue(3,LongCodec)))
Cell(Position(StringValue(iid:0444510,StringCodec) :: HNil),Content(DiscreteType,LongValue(5,LongCodec)))
Cell(Position(StringValue(iid:1004305,StringCodec) :: HNil),Content(DiscreteType,LongValue(2,LongCodec)))

scala> counts.summarise(Along(_0))(
  Mean().andThenRelocate(_.position.append("mean").toOption),
  StandardDeviation().andThenRelocate(_.position.append("sd").toOption),
  Skewness().andThenRelocate(_.position.append("skewness").toOption),
  Kurtosis().andThenRelocate(_.position.append("kurtosis").toOption)
).foreach(println)
Cell(Position(StringValue(skewness,StringCodec) :: HNil),Content(ContinuousType,DoubleValue(0.34887389949099906,DoubleCodec)))
Cell(Position(StringValue(sd,StringCodec) :: HNil),Content(ContinuousType,DoubleValue(1.6583123951777,DoubleCodec)))
Cell(Position(StringValue(kurtosis,StringCodec) :: HNil),Content(ContinuousType,DoubleValue(2.1942148760330573,DoubleCodec)))
Cell(Position(StringValue(mean,StringCodec) :: HNil),Content(ContinuousType,DoubleValue(4.0,DoubleCodec)))
```

Computing the moments can also be achieved more concisely as follows:

```
scala> counts.summarise(Along(_0))(
  Moments(
    _.append("mean").toOption,
    _.append("sd").toOption,
    _.append("skewness").toOption,
    _.append("kurtosis").toOption
  )
).foreach(println)
```

For more examples see [BasicOperations.scala](https://github.com/CommBank/grimlock/blob/master/grimlock-examples/src/main/scala/commbank/grimlock/scala/BasicOperations.scala), [Conditional.scala](https://github.com/CommBank/grimlock/blob/master/grimlock-examples/src/main/scala/commbank/grimlock/scala/Conditional.scala), [DataAnalysis.scala](https://github.com/CommBank/grimlock/blob/master/grimlock-examples/src/main/scala/commbank/grimlock/scala/DataAnalysis.scala), [DerivedData.scala](https://github.com/CommBank/grimlock/blob/master/grimlock-examples/src/main/scala/commbank/grimlock/scala/DerivedData.scala), [Ensemble.scala](https://github.com/CommBank/grimlock/blob/master/grimlock-examples/src/main/scala/commbank/grimlock/scala/Ensemble.scala), [Event.scala](https://github.com/CommBank/grimlock/blob/master/grimlock-examples/src/main/scala/commbank/grimlock/scala/Event.scala), [LabelWeighting.scala](https://github.com/CommBank/grimlock/blob/master/grimlock-examples/src/main/scala/commbank/grimlock/scala/LabelWeighting.scala), [MutualInformation.scala](https://github.com/CommBank/grimlock/blob/master/grimlock-examples/src/main/scala/commbank/grimlock/scala/MutualInformation.scala), [PipelineDataPreparation.scala](https://github.com/CommBank/grimlock/blob/master/grimlock-examples/src/main/scala/commbank/grimlock/scala/PipelineDataPreparation.scala) or [Scoring.scala](https://github.com/CommBank/grimlock/blob/master/grimlock-examples/src/main/scala/commbank/grimlock/scala/Scoring.scala).

Usage - Scalding
-----

### Setting up REPL

The examples below are executed in the Scalding REPL. To use grimlock in the REPL follow the following steps:

1. Install Scalding; follow [these](https://github.com/twitter/scalding/wiki/Getting-Started) instructions.
2. Check out tag 0.17.x; `git checkout 0.17.x`.
3. Update `build.sbt` of the scalding project. To the module `scaldingRepl`, add `grimlock` as a dependency under the `libraryDependencies`:
    `"au.com.cba.omnia" %% "grimlock-core" % "<version-string>"`;
4. Update `build.sbt` to add 'commbank-releases' to the `resolvers`:
    `"commbank-releases" at "http://commbank.artifactoryonline.com/commbank/ext-releases-local"`
5. Start REPL; `./sbt scalding-repl/console`.

After the last command, the console should appear as follows:

```
> ./sbt scalding-repl/console
...
[info] Starting scala interpreter...
[info]
import com.twitter.scalding._
import com.twitter.scalding.ReplImplicits._
import com.twitter.scalding.ReplImplicitContext._
Welcome to Scala version 2.11.8 (OpenJDK 64-Bit Server VM, Java 1.8.0_181).
Type in expressions for evaluation. Or try :help.

scala>
```

Note, for readability, the REPL info is suppressed from now on.

### Getting started

When at the Scalding REPL console, the first step is to import grimlock's functionality (be sure to press ctrl-D after the last import statement):

```
> scala> :paste
// Entering paste mode (ctrl-D to finish)

import commbank.grimlock.framework._
import commbank.grimlock.framework.encoding._
import commbank.grimlock.framework.environment.implicits._
import commbank.grimlock.framework.position._

import commbank.grimlock.library.aggregate._

import commbank.grimlock.scalding.environment._
import commbank.grimlock.scalding.environment.implicits._

import shapeless.HNil
import shapeless.nat.{ _0, _1 }
```

Next, for convenience, set up grimlock's Context as an implicit:

```
scala> implicit val context = Context()
```

The next step is to read in data (be sure to change <path to> to the correct path to the grimlock repo):

```
scala> val (data, _) = context.loadText(
  "<path to>/grimlock/grimlock-examples/src/main/scala/commbank/grimlock/data/exampleInput.txt",
  Cell.shortStringParser(StringCodec :: StringCodec :: HNil, "|")
)
```

The returned `data` is a 2 dimensional matrix. To investigate it's content Scalding's `dump` command can be used in the REPL, use grimlock's `saveAsText` API for writing to disk:

```
scala> data.dump
Cell(Position(StringValue(iid:0064402,StringCodec) :: StringValue(fid:B,StringCodec) :: HNil),Content(NominalType,StringValue(H,StringCodec)))
Cell(Position(StringValue(iid:0064402,StringCodec) :: StringValue(fid:E,StringCodec) :: HNil),Content(ContinuousType,LongValue(219,LongCodec)))
Cell(Position(StringValue(iid:0064402,StringCodec) :: StringValue(fid:H,StringCodec) :: HNil),Content(NominalType,StringValue(C,StringCodec)))
Cell(Position(StringValue(iid:0066848,StringCodec) :: StringValue(fid:A,StringCodec) :: HNil),Content(ContinuousType,LongValue(371,LongCodec)))
Cell(Position(StringValue(iid:0066848,StringCodec) :: StringValue(fid:B,StringCodec) :: HNil),Content(NominalType,StringValue(H,StringCodec)))
Cell(Position(StringValue(iid:0066848,StringCodec) :: StringValue(fid:C,StringCodec) :: HNil),Content(ContinuousType,LongValue(259,LongCodec)))
Cell(Position(StringValue(iid:0066848,StringCodec) :: StringValue(fid:D,StringCodec) :: HNil),Content(NominalType,StringValue(F,StringCodec)))
Cell(Position(StringValue(iid:0066848,StringCodec) :: StringValue(fid:E,StringCodec) :: HNil),Content(ContinuousType,LongValue(830,LongCodec)))
Cell(Position(StringValue(iid:0066848,StringCodec) :: StringValue(fid:F,StringCodec) :: HNil),Content(NominalType,StringValue(G,StringCodec)))
Cell(Position(StringValue(iid:0066848,StringCodec) :: StringValue(fid:H,StringCodec) :: HNil),Content(NominalType,StringValue(B,StringCodec)))
...
```

The following shows a number of basic operations (get number of rows, get type of features, perform simple query):

```
scala> data.measure(_0).dump
Cell(Position(LongValue(0,LongCodec) :: HNil),Content(DiscreteType,LongValue(9,LongCodec)))

scala> data.types(Over(_1))(false).dump
Cell(Position(StringValue(fid:A,StringCodec) :: HNil),Content(NominalType,TypeValue(NumericType,TypeCodec)))
Cell(Position(StringValue(fid:B,StringCodec) :: HNil),Content(NominalType,TypeValue(CategoricalType,TypeCodec)))
Cell(Position(StringValue(fid:C,StringCodec) :: HNil),Content(NominalType,TypeValue(NumericType,TypeCodec)))
Cell(Position(StringValue(fid:D,StringCodec) :: HNil),Content(NominalType,TypeValue(CategoricalType,TypeCodec)))
Cell(Position(StringValue(fid:E,StringCodec) :: HNil),Content(NominalType,TypeValue(NumericType,TypeCodec)))
Cell(Position(StringValue(fid:F,StringCodec) :: HNil),Content(NominalType,TypeValue(CategoricalType,TypeCodec)))
Cell(Position(StringValue(fid:G,StringCodec) :: HNil),Content(NominalType,TypeValue(NumericType,TypeCodec)))
Cell(Position(StringValue(fid:H,StringCodec) :: HNil),Content(NominalType,TypeValue(CategoricalType,TypeCodec)))

scala> data.which(cell => (cell.content.value gtr 995) || (cell.content.value equ "F")).dump
Position(StringValue(iid:0066848,StringCodec) :: StringValue(fid:D,StringCodec) :: HNil)
Position(StringValue(iid:0216406,StringCodec) :: StringValue(fid:E,StringCodec) :: HNil)
Position(StringValue(iid:0444510,StringCodec) :: StringValue(fid:D,StringCodec) :: HNil)
```

Now for something a little more interesting. Let's compute the number of features for each instance and then compute the moments of the distribution of counts:

```
scala> val counts = data.summarise(Over(_0))(Counts())

scala> counts.dump
Cell(Position(StringValue(iid:0064402,StringCodec) :: HNil),Content(DiscreteType,LongValue(3,LongCodec)))
Cell(Position(StringValue(iid:0066848,StringCodec) :: HNil),Content(DiscreteType,LongValue(7,LongCodec)))
Cell(Position(StringValue(iid:0216406,StringCodec) :: HNil),Content(DiscreteType,LongValue(5,LongCodec)))
Cell(Position(StringValue(iid:0221707,StringCodec) :: HNil),Content(DiscreteType,LongValue(4,LongCodec)))
Cell(Position(StringValue(iid:0262443,StringCodec) :: HNil),Content(DiscreteType,LongValue(2,LongCodec)))
Cell(Position(StringValue(iid:0364354,StringCodec) :: HNil),Content(DiscreteType,LongValue(5,LongCodec)))
Cell(Position(StringValue(iid:0375226,StringCodec) :: HNil),Content(DiscreteType,LongValue(3,LongCodec)))
Cell(Position(StringValue(iid:0444510,StringCodec) :: HNil),Content(DiscreteType,LongValue(5,LongCodec)))
Cell(Position(StringValue(iid:1004305,StringCodec) :: HNil),Content(DiscreteType,LongValue(2,LongCodec)))

scala> counts.summarise(Along(_0))(
  Mean().andThenRelocate(_.position.append("mean").toOption),
  StandardDeviation().andThenRelocate(_.position.append("sd").toOption),
  Skewness().andThenRelocate(_.position.append("skewness").toOption),
  Kurtosis().andThenRelocate(_.position.append("kurtosis").toOption)
).dump
Cell(Position(StringValue(skewness,StringCodec) :: HNil),Content(ContinuousType,DoubleValue(0.34887389949099906,DoubleCodec)))
Cell(Position(StringValue(sd,StringCodec) :: HNil),Content(ContinuousType,DoubleValue(1.6583123951777,DoubleCodec)))
Cell(Position(StringValue(kurtosis,StringCodec) :: HNil),Content(ContinuousType,DoubleValue(2.1942148760330573,DoubleCodec)))
Cell(Position(StringValue(mean,StringCodec) :: HNil),Content(ContinuousType,DoubleValue(4.0,DoubleCodec)))
```

Computing the moments can also be achieved more concisely as follows:

```
scala> counts.summarise(Along(_0))(
  Moments(
    _.append("mean").toOption,
    _.append("sd").toOption,
    _.append("skewness").toOption,
    _.append("kurtosis").toOption
  )
).dump
```

For more examples see [BasicOperations.scala](https://github.com/CommBank/grimlock/blob/master/grimlock-examples/src/main/scala/commbank/grimlock/scalding/BasicOperations.scala), [Conditional.scala](https://github.com/CommBank/grimlock/blob/master/grimlock-examples/src/main/scala/commbank/grimlock/scalding/Conditional.scala), [DataAnalysis.scala](https://github.com/CommBank/grimlock/blob/master/grimlock-examples/src/main/scala/commbank/grimlock/scalding/DataAnalysis.scala), [DerivedData.scala](https://github.com/CommBank/grimlock/blob/master/grimlock-examples/src/main/scala/commbank/grimlock/scalding/DerivedData.scala), [Ensemble.scala](https://github.com/CommBank/grimlock/blob/master/grimlock-examples/src/main/scala/commbank/grimlock/scalding/Ensemble.scala), [Event.scala](https://github.com/CommBank/grimlock/blob/master/grimlock-examples/src/main/scala/commbank/grimlock/scalding/Event.scala), [LabelWeighting.scala](https://github.com/CommBank/grimlock/blob/master/grimlock-examples/src/main/scala/commbank/grimlock/scalding/LabelWeighting.scala), [MutualInformation.scala](https://github.com/CommBank/grimlock/blob/master/grimlock-examples/src/main/scala/commbank/grimlock/scalding/MutualInformation.scala), [PipelineDataPreparation.scala](https://github.com/CommBank/grimlock/blob/master/grimlock-examples/src/main/scala/commbank/grimlock/scalding/PipelineDataPreparation.scala) or [Scoring.scala](https://github.com/CommBank/grimlock/blob/master/grimlock-examples/src/main/scala/commbank/grimlock/scalding/Scoring.scala).

Usage - Spark
-----

### Setting up REPL

The examples below are executed in the Spark REPL. To use grimlock in the REPL follow the following steps:

1. Download the latest source code release for Spark from [here](http://spark.apache.org/downloads.html).
2. You can, optionally, suppress much of the console INFO output. Follow [these](http://stackoverflow.com/questions/28189408/how-to-reduce-the-verbosity-of-sparks-runtime-output) instructions.
3. Start REPL; `./bin/spark-shell --master local --jars <path to>/grimlock-core_2.11-assembly.jar`.

After the last command, the console should appear as follows:

```
> ./bin/spark-shell --master local --jars <path to>/grimlock-core_2.11-assembly.jar
...
Spark context available as sc (master = local, ...).
Spark session available as 'spark'
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 2.3.2
      /_/

Using Scala version 2.11.8 (OpenJDK 64-Bit Server VM, Java 1.8.0_181)
Type in expressions to have them evaluated.
Type :help for more information.

scala>
```

Note, for readability, the REPL info is suppressed from now on.

### Getting started

When at the Spark REPL console, the first step is to import grimlock's functionality (be sure to press ctrl-D after the last import statement):

```
> scala> :paste
// Entering paste mode (ctrl-D to finish)

import commbank.grimlock.framework._
import commbank.grimlock.framework.encoding._
import commbank.grimlock.framework.environment.implicits._
import commbank.grimlock.framework.position._

import commbank.grimlock.library.aggregate._

import commbank.grimlock.spark.environment._
import commbank.grimlock.spark.environment.implicits._

import shapeless.HNil
import shapeless.nat.{ _0, _1 }
```

Next, for convenience, set up grimlock's Context as an implicit:

```
scala> implicit val context = Context(spark)
```

The next step is to read in data (be sure to change <path to> to the correct path to the grimlock repo):

```
scala> val (data, _) = context.loadText(
  "<path to>/grimlock/grimlock-examples/src/main/scala/commbank/grimlock/data/exampleInput.txt",
  Cell.shortStringParser(StringCodec :: StringCodec :: HNil, "|")
)
```

The returned `data` is a 2 dimensional matrix. To investigate it's content Spark's `foreach` command can be used in the REPL, use the grimlock's `saveAsText` API for writing to disk:

```
scala> data.foreach(println)
Cell(Position(StringValue(iid:0064402,StringCodec) :: StringValue(fid:B,StringCodec) :: HNil),Content(NominalType,StringValue(H,StringCodec)))
Cell(Position(StringValue(iid:0064402,StringCodec) :: StringValue(fid:E,StringCodec) :: HNil),Content(ContinuousType,LongValue(219,LongCodec)))
Cell(Position(StringValue(iid:0064402,StringCodec) :: StringValue(fid:H,StringCodec) :: HNil),Content(NominalType,StringValue(C,StringCodec)))
Cell(Position(StringValue(iid:0066848,StringCodec) :: StringValue(fid:A,StringCodec) :: HNil),Content(ContinuousType,LongValue(371,LongCodec)))
Cell(Position(StringValue(iid:0066848,StringCodec) :: StringValue(fid:B,StringCodec) :: HNil),Content(NominalType,StringValue(H,StringCodec)))
Cell(Position(StringValue(iid:0066848,StringCodec) :: StringValue(fid:C,StringCodec) :: HNil),Content(ContinuousType,LongValue(259,LongCodec)))
Cell(Position(StringValue(iid:0066848,StringCodec) :: StringValue(fid:D,StringCodec) :: HNil),Content(NominalType,StringValue(F,StringCodec)))
Cell(Position(StringValue(iid:0066848,StringCodec) :: StringValue(fid:E,StringCodec) :: HNil),Content(ContinuousType,LongValue(830,LongCodec)))
Cell(Position(StringValue(iid:0066848,StringCodec) :: StringValue(fid:F,StringCodec) :: HNil),Content(NominalType,StringValue(G,StringCodec)))
Cell(Position(StringValue(iid:0066848,StringCodec) :: StringValue(fid:H,StringCodec) :: HNil),Content(NominalType,StringValue(B,StringCodec)))
...
```

The following shows a number of basic operations (get number of rows, get type of features, perform simple query):

```
scala> data.measure(_0).foreach(println)
Cell(Position(LongValue(0,LongCodec) :: HNil),Content(DiscreteType,LongValue(9,LongCodec)))

scala> data.types(Over(_1))(false).foreach(println)
Cell(Position(StringValue(fid:G,StringCodec) :: HNil),Content(NominalType,TypeValue(NumericType,TypeCodec)))
Cell(Position(StringValue(fid:D,StringCodec) :: HNil),Content(NominalType,TypeValue(CategoricalType,TypeCodec)))
Cell(Position(StringValue(fid:E,StringCodec) :: HNil),Content(NominalType,TypeValue(NumericType,TypeCodec)))
Cell(Position(StringValue(fid:A,StringCodec) :: HNil),Content(NominalType,TypeValue(NumericType,TypeCodec)))
Cell(Position(StringValue(fid:B,StringCodec) :: HNil),Content(NominalType,TypeValue(CategoricalType,TypeCodec)))
Cell(Position(StringValue(fid:C,StringCodec) :: HNil),Content(NominalType,TypeValue(NumericType,TypeCodec)))
Cell(Position(StringValue(fid:H,StringCodec) :: HNil),Content(NominalType,TypeValue(CategoricalType,TypeCodec)))
Cell(Position(StringValue(fid:F,StringCodec) :: HNil),Content(NominalType,TypeValue(CategoricalType,TypeCodec)))

scala> data.which(cell => (cell.content.value gtr 995) || (cell.content.value equ "F")).foreach(println)
Position(StringValue(iid:0066848,StringCodec) :: StringValue(fid:D,StringCodec) :: HNil)
Position(StringValue(iid:0216406,StringCodec) :: StringValue(fid:E,StringCodec) :: HNil)
Position(StringValue(iid:0444510,StringCodec) :: StringValue(fid:D,StringCodec) :: HNil)
```

Now for something a little more interesting. Let's compute the number of features for each instance and then compute the moments of the distribution of counts:

```
scala> val counts = data.summarise(Over(_0))(Counts())

scala> counts.foreach(println)
Cell(Position(StringValue(iid:0221707,StringCodec) :: HNil),Content(DiscreteType,LongValue(4,LongCodec)))
Cell(Position(StringValue(iid:0444510,StringCodec) :: HNil),Content(DiscreteType,LongValue(5,LongCodec)))
Cell(Position(StringValue(iid:0064402,StringCodec) :: HNil),Content(DiscreteType,LongValue(3,LongCodec)))
Cell(Position(StringValue(iid:0375226,StringCodec) :: HNil),Content(DiscreteType,LongValue(3,LongCodec)))
Cell(Position(StringValue(iid:0262443,StringCodec) :: HNil),Content(DiscreteType,LongValue(2,LongCodec)))
Cell(Position(StringValue(iid:0216406,StringCodec) :: HNil),Content(DiscreteType,LongValue(5,LongCodec)))
Cell(Position(StringValue(iid:0066848,StringCodec) :: HNil),Content(DiscreteType,LongValue(7,LongCodec)))
Cell(Position(StringValue(iid:1004305,StringCodec) :: HNil),Content(DiscreteType,LongValue(2,LongCodec)))
Cell(Position(StringValue(iid:0364354,StringCodec) :: HNil),Content(DiscreteType,LongValue(5,LongCodec)))

scala> counts.summarise(Along(_0))(
  Mean().andThenRelocate(_.position.append("mean").toOption),
  StandardDeviation().andThenRelocate(_.position.append("sd").toOption),
  Skewness().andThenRelocate(_.position.append("skewness").toOption),
  Kurtosis().andThenRelocate(_.position.append("kurtosis").toOption)
).foreach(println)
Cell(Position(StringValue(skewness,StringCodec) :: HNil),Content(ContinuousType,DoubleValue(0.34887389949099906,DoubleCodec)))
Cell(Position(StringValue(sd,StringCodec) :: HNil),Content(ContinuousType,DoubleValue(1.6583123951777,DoubleCodec)))
Cell(Position(StringValue(kurtosis,StringCodec) :: HNil),Content(ContinuousType,DoubleValue(2.194214876033058,DoubleCodec)))
Cell(Position(StringValue(mean,StringCodec) :: HNil),Content(ContinuousType,DoubleValue(4.0,DoubleCodec)))
```

Computing the moments can also be achieved more concisely as follows:

```
scala> counts.summarise(Along(_0))(
  Moments(
    _.append("mean").toOption,
    _.append("sd").toOption,
    _.append("skewness").toOption,
    _.append("kurtosis").toOption
  )
).foreach(println)
```

For more examples see [BasicOperations.scala](https://github.com/CommBank/grimlock/blob/master/grimlock-examples/src/main/scala/commbank/grimlock/spark/BasicOperations.scala), [Conditional.scala](https://github.com/CommBank/grimlock/blob/master/grimlock-examples/src/main/scala/commbank/grimlock/spark/Conditional.scala), [DataAnalysis.scala](https://github.com/CommBank/grimlock/blob/master/grimlock-examples/src/main/scala/commbank/grimlock/spark/DataAnalysis.scala), [DerivedData.scala](https://github.com/CommBank/grimlock/blob/master/grimlock-examples/src/main/scala/commbank/grimlock/spark/DerivedData.scala), [Ensemble.scala](https://github.com/CommBank/grimlock/blob/master/grimlock-examples/src/main/scala/commbank/grimlock/spark/Ensemble.scala), [Event.scala](https://github.com/CommBank/grimlock/blob/master/grimlock-examples/src/main/scala/commbank/grimlock/spark/Event.scala), [LabelWeighting.scala](https://github.com/CommBank/grimlock/blob/master/grimlock-examples/src/main/scala/commbank/grimlock/spark/LabelWeighting.scala), [MutualInformation.scala](https://github.com/CommBank/grimlock/blob/master/grimlock-examples/src/main/scala/commbank/grimlock/spark/MutualInformation.scala), [PipelineDataPreparation.scala](https://github.com/CommBank/grimlock/blob/master/grimlock-examples/src/main/scala/commbank/grimlock/spark/PipelineDataPreparation.scala) or [Scoring.scala](https://github.com/CommBank/grimlock/blob/master/grimlock-examples/src/main/scala/commbank/grimlock/spark/Scoring.scala).

