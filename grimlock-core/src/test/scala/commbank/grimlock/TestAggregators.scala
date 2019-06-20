// Copyright 2015,2016,2017,2018,2019 Commonwealth Bank of Australia
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package commbank.grimlock.test

import commbank.grimlock.framework._
import commbank.grimlock.framework.content._
import commbank.grimlock.framework.encoding._
import commbank.grimlock.framework.environment.implicits._
import commbank.grimlock.framework.extract._
import commbank.grimlock.framework.metadata._
import commbank.grimlock.framework.position._

import commbank.grimlock.library.aggregate._

import shapeless.{ ::, HList, HNil, Nat }
import shapeless.nat.{ _0, _1, _3 }

trait TestAggregators extends TestGrimlock {
  type P = Value[String] :: Value[String] :: HNil
  type S = Value[String] :: HNil

  /** Append a string to the position. */
  def appendString[
    Z <: HList
  ](
    str: String
  )(implicit
    ev1: Value.Box[String],
    ev2: Position.AppendConstraints[Z, Value[String]]
  ): Locate.FromPosition[Z, ev2.Q] = (pos: Position[Z]) => pos.append(str).toOption

  def getBooleanContent(value: Boolean): Content = Content(NominalSchema[Boolean](), value)
  def getLongContent(value: Long): Content = Content(DiscreteSchema[Long](), value)
  def getDoubleContent(value: Double): Content = Content(ContinuousSchema[Double](), value)
  def getStringContent(value: String): Content = Content(NominalSchema[String](), value)
}

class TestCounts extends TestAggregators {
  val cell1 = Cell(Position("foo", "one"), getDoubleContent(1))
  val cell2 = Cell(Position("foo", "two"), getDoubleContent(2))

  "A Counts" should "prepare, reduce and present" in {
    val obj = Counts[P, S]()

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(1)

    val r = obj.reduce(t1.get, t2.get)
    r shouldBe 2

    val c = obj.present(Position("foo"), r).result
    c shouldBe Option(Cell(Position("foo"), getLongContent(2)))
  }

  it should "prepare, reduce and present expanded" in {
    val obj = Counts[P, S]().andThenRelocate(_.position.append("count").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(1)

    val r = obj.reduce(t1.get, t2.get)
    r shouldBe 2

    val c = obj.present(Position("foo"), r).result
    c shouldBe Option(Cell(Position("foo", "count"), getLongContent(2)))
  }
}

class TestMean extends TestAggregators {
  val cell1 = Cell(Position("foo", "one"), getDoubleContent(1))
  val cell2 = Cell(Position("foo", "two"), getDoubleContent(2))
  val cell3 = Cell(Position("foo", "bar"), getStringContent("bar"))

  "A Mean" should "prepare, reduce and present" in {
    val obj = Mean[P, S]()

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(com.twitter.algebird.Moments(1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(com.twitter.algebird.Moments(2))

    val r = obj.reduce(t1.get, t2.get)
    r shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.present(Position("foo"), r).result
    c shouldBe Option(Cell(Position("foo"), getDoubleContent(1.5)))
  }

  it should "prepare, reduce and present with strict and nan" in {
    val obj = Mean[P, S](false, true, true)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(com.twitter.algebird.Moments(1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(com.twitter.algebird.Moments(2))

    val t3 = obj.prepare(cell3)
    t3.map(_.mean.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3.get)
    r2.mean.compare(Double.NaN) shouldBe 0

    val c = obj.present(Position("foo"), r2).result.get
    c.position shouldBe Position("foo")
    c.content.value.as[Double].map(_.compare(Double.NaN)) shouldBe Option(0)
  }

  it should "prepare, reduce and present with strict and non-nan" in {
    val obj = Mean[P, S](false, true, false)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(com.twitter.algebird.Moments(1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(com.twitter.algebird.Moments(2))

    val t3 = obj.prepare(cell3)
    t3.map(_.mean.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3.get)
    r2.mean.compare(Double.NaN) shouldBe 0

    val c = obj.present(Position("foo"), r2).result
    c shouldBe None
  }

  it should "prepare, reduce and present with non-strict and nan" in {
    val obj = Mean[P, S](false, false, true)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(com.twitter.algebird.Moments(1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(com.twitter.algebird.Moments(2))

    val t3 = obj.prepare(cell3)
    t3.map(_.mean.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.present(Position("foo"), r2).result
    c shouldBe Option(Cell(Position("foo"), getDoubleContent(1.5)))
  }

  it should "prepare, reduce and present with non-strict and non-nan" in {
    val obj = Mean[P, S](false, false, false)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(com.twitter.algebird.Moments(1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(com.twitter.algebird.Moments(2))

    val t3 = obj.prepare(cell3)
    t3.map(_.mean.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.present(Position("foo"), r2).result
    c shouldBe Option(Cell(Position("foo"), getDoubleContent(1.5)))
  }

  it should "prepare, reduce and present expanded" in {
    val obj = Mean[P, S]().andThenRelocate(_.position.append("mean").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(com.twitter.algebird.Moments(1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(com.twitter.algebird.Moments(2))

    val r = obj.reduce(t1.get, t2.get)
    r shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.present(Position("foo"), r).result
    c shouldBe Option(Cell(Position("foo", "mean"), getDoubleContent(1.5)))
  }

  it should "prepare, reduce and present expanded with strict and nan" in {
    val obj = Mean[P, S](false, true, true).andThenRelocate(_.position.append("mean").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(com.twitter.algebird.Moments(1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(com.twitter.algebird.Moments(2))

    val t3 = obj.prepare(cell3)
    t3.map(_.mean.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3.get)
    r2.mean.compare(Double.NaN) shouldBe 0

    val c = obj.present(Position("foo"), r2).result.get
    c.position shouldBe Position("foo", "mean")
    c.content.value.as[Double].map(_.compare(Double.NaN)) shouldBe Option(0)
  }

  it should "prepare, reduce and present expanded with strict and non-nan" in {
    val obj = Mean[P, S](false, true, false).andThenRelocate(_.position.append("mean").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(com.twitter.algebird.Moments(1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(com.twitter.algebird.Moments(2))

    val t3 = obj.prepare(cell3)
    t3.map(_.mean.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3.get)
    r2.mean.compare(Double.NaN) shouldBe 0

    val c = obj.present(Position("foo"), r2).result
    c shouldBe None
  }

  it should "prepare, reduce and present expanded with non-strict and nan" in {
    val obj = Mean[P, S](false, false, true).andThenRelocate(_.position.append("mean").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(com.twitter.algebird.Moments(1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(com.twitter.algebird.Moments(2))

    val t3 = obj.prepare(cell3)
    t3.map(_.mean.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.present(Position("foo"), r2).result
    c shouldBe Option(Cell(Position("foo", "mean"), getDoubleContent(1.5)))
  }

  it should "prepare, reduce and present expanded with non-strict and non-nan" in {
    val obj = Mean[P, S](false, false, false).andThenRelocate(_.position.append("mean").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(com.twitter.algebird.Moments(1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(com.twitter.algebird.Moments(2))

    val t3 = obj.prepare(cell3)
    t3.map(_.mean.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.present(Position("foo"), r2).result
    c shouldBe Option(Cell(Position("foo", "mean"), getDoubleContent(1.5)))
  }

  it should "filter" in {
    Mean[P, S](true).prepare(cell1) shouldBe Option(com.twitter.algebird.Moments(1))
    Mean[P, S](false).prepare(cell1) shouldBe Option(com.twitter.algebird.Moments(1))
    Mean[P, S](true).prepare(cell3) shouldBe None
    Mean[P, S](false).prepare(cell3).map(_.mean.compare(Double.NaN)) shouldBe Option(0)
  }
}

class TestStandardDeviation extends TestAggregators {
  val cell1 = Cell(Position("foo", "one"), getDoubleContent(1))
  val cell2 = Cell(Position("foo", "two"), getDoubleContent(2))
  val cell3 = Cell(Position("foo", "bar"), getStringContent("bar"))

  "A StandardDeviation" should "prepare, reduce and present" in {
    val obj = StandardDeviation[P, S]()

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(com.twitter.algebird.Moments(1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(com.twitter.algebird.Moments(2))

    val r = obj.reduce(t1.get, t2.get)
    r shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.present(Position("foo"), r).result
    c shouldBe Option(Cell(Position("foo"), getDoubleContent(0.7071067811865476)))
  }

  it should "prepare, reduce and present with strict and nan" in {
    val obj = StandardDeviation[P, S](true, false, true, true)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(com.twitter.algebird.Moments(1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(com.twitter.algebird.Moments(2))

    val t3 = obj.prepare(cell3)
    t3.map(_.mean.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3.get)
    r2.mean.compare(Double.NaN) shouldBe 0

    val c = obj.present(Position("foo"), r2).result.get
    c.position shouldBe Position("foo")
    c.content.value.as[Double].map(_.compare(Double.NaN)) shouldBe Option(0)
  }

  it should "prepare, reduce and present with strict and non-nan" in {
    val obj = StandardDeviation[P, S](false, false, true, false)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(com.twitter.algebird.Moments(1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(com.twitter.algebird.Moments(2))

    val t3 = obj.prepare(cell3)
    t3.map(_.mean.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3.get)
    r2.mean.compare(Double.NaN) shouldBe 0

    val c = obj.present(Position("foo"), r2).result
    c shouldBe None
  }

  it should "prepare, reduce and present with non-strict and nan" in {
    val obj = StandardDeviation[P, S](true, false, false, true)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(com.twitter.algebird.Moments(1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(com.twitter.algebird.Moments(2))

    val t3 = obj.prepare(cell3)
    t3.map(_.mean.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.present(Position("foo"), r2).result
    c shouldBe Option(Cell(Position("foo"), getDoubleContent(0.5)))
  }

  it should "prepare, reduce and present with non-strict and non-nan" in {
    val obj = StandardDeviation[P, S](false, false, false, false)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(com.twitter.algebird.Moments(1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(com.twitter.algebird.Moments(2))

    val t3 = obj.prepare(cell3)
    t3.map(_.mean.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.present(Position("foo"), r2).result
    c shouldBe Option(Cell(Position("foo"), getDoubleContent(0.7071067811865476)))
  }

  it should "prepare, reduce and present expanded" in {
    val obj = StandardDeviation[P, S]().andThenRelocate(_.position.append("sd").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(com.twitter.algebird.Moments(1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(com.twitter.algebird.Moments(2))

    val r = obj.reduce(t1.get, t2.get)
    r shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.present(Position("foo"), r).result
    c shouldBe Option(Cell(Position("foo", "sd"), getDoubleContent(0.7071067811865476)))
  }

  it should "prepare, reduce and present expanded with strict and nan" in {
    val obj = StandardDeviation[P, S](true, false, true, true).andThenRelocate(_.position.append("sd").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(com.twitter.algebird.Moments(1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(com.twitter.algebird.Moments(2))

    val t3 = obj.prepare(cell3)
    t3.map(_.mean.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3.get)
    r2.mean.compare(Double.NaN) shouldBe 0

    val c = obj.present(Position("foo"), r2).result.get
    c.position shouldBe Position("foo", "sd")
    c.content.value.as[Double].map(_.compare(Double.NaN)) shouldBe Option(0)
  }

  it should "prepare, reduce and present expanded with strict and non-nan" in {
    val obj = StandardDeviation[P, S](false, false, true, false).andThenRelocate(_.position.append("sd").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(com.twitter.algebird.Moments(1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(com.twitter.algebird.Moments(2))

    val t3 = obj.prepare(cell3)
    t3.map(_.mean.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3.get)
    r2.mean.compare(Double.NaN) shouldBe 0

    val c = obj.present(Position("foo"), r2).result
    c shouldBe None
  }

  it should "prepare, reduce and present expanded with non-strict and nan" in {
    val obj = StandardDeviation[P, S](true, false, false, true).andThenRelocate(_.position.append("sd").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(com.twitter.algebird.Moments(1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(com.twitter.algebird.Moments(2))

    val t3 = obj.prepare(cell3)
    t3.map(_.mean.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.present(Position("foo"), r2).result
    c shouldBe Option(Cell(Position("foo", "sd"), getDoubleContent(0.5)))
  }

  it should "prepare, reduce and present expanded with non-strict and non-nan" in {
    val obj = StandardDeviation[P, S](false, false, false, false).andThenRelocate(_.position.append("sd").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(com.twitter.algebird.Moments(1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(com.twitter.algebird.Moments(2))

    val t3 = obj.prepare(cell3)
    t3.map(_.mean.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.present(Position("foo"), r2).result
    c shouldBe Option(Cell(Position("foo", "sd"), getDoubleContent(0.7071067811865476)))
  }

  it should "filter" in {
    StandardDeviation[P, S](filter=true).prepare(cell1) shouldBe Option(com.twitter.algebird.Moments(1))
    StandardDeviation[P, S](filter=false).prepare(cell1) shouldBe Option(com.twitter.algebird.Moments(1))
    StandardDeviation[P, S](filter=true).prepare(cell3) shouldBe None
    StandardDeviation[P, S](filter=false).prepare(cell3).map(_.mean.compare(Double.NaN)) shouldBe Option(0)
  }
}

class TestSkewness extends TestAggregators {
  val cell1 = Cell(Position("foo", "one"), getDoubleContent(1))
  val cell2 = Cell(Position("foo", "two"), getDoubleContent(2))
  val cell3 = Cell(Position("foo", "three"), getDoubleContent(3))
  val cell4 = Cell(Position("foo", "bar"), getStringContent("bar"))

  "A Skewness" should "prepare, reduce and present" in {
    val obj = Skewness[P, S]()

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(com.twitter.algebird.Moments(1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(com.twitter.algebird.Moments(2))

    val t3 = obj.prepare(cell3)
    t3 shouldBe Option(com.twitter.algebird.Moments(3))

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe com.twitter.algebird.Moments(3, 2.0, 2.0, 0.0, 2.0)

    val c = obj.present(Position("foo"), r2).result
    c shouldBe Option(Cell(Position("foo"), getDoubleContent(0)))
  }

  it should "prepare, reduce and present with strict and nan" in {
    val obj = Skewness[P, S](false, true, true)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(com.twitter.algebird.Moments(1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(com.twitter.algebird.Moments(2))

    val t3 = obj.prepare(cell3)
    t3 shouldBe Option(com.twitter.algebird.Moments(3))

    val t4 = obj.prepare(cell4)
    t4.map(_.mean.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe com.twitter.algebird.Moments(3, 2.0, 2.0, 0.0, 2.0)

    val r3 = obj.reduce(r2, t4.get)
    r3.mean.compare(Double.NaN) shouldBe 0

    val c = obj.present(Position("foo"), r3).result.get
    c.position shouldBe Position("foo")
    c.content.value.as[Double].map(_.compare(Double.NaN)) shouldBe Option(0)
  }

  it should "prepare, reduce and present with strict and non-nan" in {
    val obj = Skewness[P, S](false, true, false)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(com.twitter.algebird.Moments(1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(com.twitter.algebird.Moments(2))

    val t3 = obj.prepare(cell3)
    t3 shouldBe Option(com.twitter.algebird.Moments(3))

    val t4 = obj.prepare(cell4)
    t4.map(_.mean.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe com.twitter.algebird.Moments(3, 2.0, 2.0, 0.0, 2.0)

    val r3 = obj.reduce(r2, t4.get)
    r3.mean.compare(Double.NaN) shouldBe 0

    val c = obj.present(Position("foo"), r3).result
    c shouldBe None
  }

  it should "prepare, reduce and present with non-strict and nan" in {
    val obj = Skewness[P, S](false, false, true)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(com.twitter.algebird.Moments(1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(com.twitter.algebird.Moments(2))

    val t3 = obj.prepare(cell3)
    t3 shouldBe Option(com.twitter.algebird.Moments(3))

    val t4 = obj.prepare(cell4)
    t4.map(_.mean.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe com.twitter.algebird.Moments(3, 2.0, 2.0, 0.0, 2.0)

    val r3 = obj.reduce(r2, t4.get)
    r3 shouldBe com.twitter.algebird.Moments(3, 2.0, 2.0, 0.0, 2.0)

    val c = obj.present(Position("foo"), r3).result
    c shouldBe Option(Cell(Position("foo"), getDoubleContent(0)))
  }

  it should "prepare, reduce and present with non-strict and non-nan" in {
    val obj = Skewness[P, S](false, false, false)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(com.twitter.algebird.Moments(1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(com.twitter.algebird.Moments(2))

    val t3 = obj.prepare(cell3)
    t3 shouldBe Option(com.twitter.algebird.Moments(3))

    val t4 = obj.prepare(cell4)
    t4.map(_.mean.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe com.twitter.algebird.Moments(3, 2.0, 2.0, 0.0, 2.0)

    val r3 = obj.reduce(r2, t4.get)
    r3 shouldBe com.twitter.algebird.Moments(3, 2.0, 2.0, 0.0, 2.0)

    val c = obj.present(Position("foo"), r3).result
    c shouldBe Option(Cell(Position("foo"), getDoubleContent(0)))
  }

  it should "prepare, reduce and present expanded" in {
    val obj = Skewness[P, S]().andThenRelocate(_.position.append("skewness").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(com.twitter.algebird.Moments(1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(com.twitter.algebird.Moments(2))

    val t3 = obj.prepare(cell3)
    t3 shouldBe Option(com.twitter.algebird.Moments(3))

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe com.twitter.algebird.Moments(3, 2.0, 2.0, 0.0, 2.0)

    val c = obj.present(Position("foo"), r2).result
    c shouldBe Option(Cell(Position("foo", "skewness"), getDoubleContent(0)))
  }

  it should "prepare, reduce and present expanded with strict and nan" in {
    val obj = Skewness[P, S](false, true, true).andThenRelocate(_.position.append("skewness").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(com.twitter.algebird.Moments(1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(com.twitter.algebird.Moments(2))

    val t3 = obj.prepare(cell3)
    t3 shouldBe Option(com.twitter.algebird.Moments(3))

    val t4 = obj.prepare(cell4)
    t4.map(_.mean.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe com.twitter.algebird.Moments(3, 2.0, 2.0, 0.0, 2.0)

    val r3 = obj.reduce(r2, t4.get)
    r3.mean.compare(Double.NaN) shouldBe 0

    val c = obj.present(Position("foo"), r3).result.get
    c.position shouldBe Position("foo", "skewness")
    c.content.value.as[Double].map(_.compare(Double.NaN)) shouldBe Option(0)
  }

  it should "prepare, reduce and present expanded with strict and non-nan" in {
    val obj = Skewness[P, S](false, true, false).andThenRelocate(_.position.append("skewness").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(com.twitter.algebird.Moments(1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(com.twitter.algebird.Moments(2))

    val t3 = obj.prepare(cell3)
    t3 shouldBe Option(com.twitter.algebird.Moments(3))

    val t4 = obj.prepare(cell4)
    t4.map(_.mean.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe com.twitter.algebird.Moments(3, 2.0, 2.0, 0.0, 2.0)

    val r3 = obj.reduce(r2, t4.get)
    r3.mean.compare(Double.NaN) shouldBe 0

    val c = obj.present(Position("foo"), r3).result
    c shouldBe None
  }

  it should "prepare, reduce and present expanded with non-strict and nan" in {
    val obj = Skewness[P, S](false, false, true).andThenRelocate(_.position.append("skewness").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(com.twitter.algebird.Moments(1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(com.twitter.algebird.Moments(2))

    val t3 = obj.prepare(cell3)
    t3 shouldBe Option(com.twitter.algebird.Moments(3))

    val t4 = obj.prepare(cell4)
    t4.map(_.mean.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe com.twitter.algebird.Moments(3, 2.0, 2.0, 0.0, 2.0)

    val r3 = obj.reduce(r2, t4.get)
    r3 shouldBe com.twitter.algebird.Moments(3, 2.0, 2.0, 0.0, 2.0)

    val c = obj.present(Position("foo"), r3).result
    c shouldBe Option(Cell(Position("foo", "skewness"), getDoubleContent(0)))
  }

  it should "prepare, reduce and present expanded with non-strict and non-nan" in {
    val obj = Skewness[P, S](false, false, false).andThenRelocate(_.position.append("skewness").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(com.twitter.algebird.Moments(1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(com.twitter.algebird.Moments(2))

    val t3 = obj.prepare(cell3)
    t3 shouldBe Option(com.twitter.algebird.Moments(3))

    val t4 = obj.prepare(cell4)
    t4.map(_.mean.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe com.twitter.algebird.Moments(3, 2.0, 2.0, 0.0, 2.0)

    val r3 = obj.reduce(r2, t4.get)
    r3 shouldBe com.twitter.algebird.Moments(3, 2.0, 2.0, 0.0, 2.0)

    val c = obj.present(Position("foo"), r3).result
    c shouldBe Option(Cell(Position("foo", "skewness"), getDoubleContent(0)))
  }

  it should "filter" in {
    Skewness[P, S](true).prepare(cell1) shouldBe Option(com.twitter.algebird.Moments(1))
    Skewness[P, S](false).prepare(cell1) shouldBe Option(com.twitter.algebird.Moments(1))
    Skewness[P, S](true).prepare(cell4) shouldBe None
    Skewness[P, S](false).prepare(cell4).map(_.mean.compare(Double.NaN)) shouldBe Option(0)
  }
}

class TestKurtosis extends TestAggregators {
  val cell1 = Cell(Position("foo", "one"), getDoubleContent(1))
  val cell2 = Cell(Position("foo", "two"), getDoubleContent(2))
  val cell3 = Cell(Position("foo", "three"), getDoubleContent(3))
  val cell4 = Cell(Position("foo", "four"), getDoubleContent(4))
  val cell5 = Cell(Position("foo", "bar"), getStringContent("bar"))

  "A Kurtosis" should "prepare, reduce and present" in {
    val obj = Kurtosis[P, S]()

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(com.twitter.algebird.Moments(1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(com.twitter.algebird.Moments(2))

    val t3 = obj.prepare(cell3)
    t3 shouldBe Option(com.twitter.algebird.Moments(3))

    val t4 = obj.prepare(cell4)
    t4 shouldBe Option(com.twitter.algebird.Moments(4))

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe com.twitter.algebird.Moments(3, 2.0, 2.0, 0.0, 2.0)

    val r3 = obj.reduce(r2, t4.get)
    r3 shouldBe com.twitter.algebird.Moments(4, 2.5, 5.0, 0.0, 10.25)

    val c = obj.present(Position("foo"), r3).result
    c shouldBe Option(Cell(Position("foo"), getDoubleContent(1.64)))
  }

  it should "prepare, reduce and present with strict and nan" in {
    val obj = Kurtosis[P, S](true, false, true, true)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(com.twitter.algebird.Moments(1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(com.twitter.algebird.Moments(2))

    val t3 = obj.prepare(cell3)
    t3 shouldBe Option(com.twitter.algebird.Moments(3))

    val t4 = obj.prepare(cell4)
    t4 shouldBe Option(com.twitter.algebird.Moments(4))

    val t5 = obj.prepare(cell5)
    t5.map(_.mean.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe com.twitter.algebird.Moments(3, 2.0, 2.0, 0.0, 2.0)

    val r3 = obj.reduce(r2, t4.get)
    r3 shouldBe com.twitter.algebird.Moments(4, 2.5, 5.0, 0.0, 10.25)

    val r4 = obj.reduce(r3, t5.get)
    r4.mean.compare(Double.NaN) shouldBe 0

    val c = obj.present(Position("foo"), r4).result.get
    c.position shouldBe Position("foo")
    c.content.value.as[Double].map(_.compare(Double.NaN)) shouldBe Option(0)
  }

  it should "prepare, reduce and present with strict and non-nan" in {
    val obj = Kurtosis[P, S](false, false, true, false)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(com.twitter.algebird.Moments(1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(com.twitter.algebird.Moments(2))

    val t3 = obj.prepare(cell3)
    t3 shouldBe Option(com.twitter.algebird.Moments(3))

    val t4 = obj.prepare(cell4)
    t4 shouldBe Option(com.twitter.algebird.Moments(4))

    val t5 = obj.prepare(cell5)
    t5.map(_.mean.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe com.twitter.algebird.Moments(3, 2.0, 2.0, 0.0, 2.0)

    val r3 = obj.reduce(r2, t4.get)
    r3 shouldBe com.twitter.algebird.Moments(4, 2.5, 5.0, 0.0, 10.25)

    val r4 = obj.reduce(r3, t5.get)
    r4.mean.compare(Double.NaN) shouldBe 0

    val c = obj.present(Position("foo"), r4).result
    c shouldBe None
  }

  it should "prepare, reduce and present with non-strict and nan" in {
    val obj = Kurtosis[P, S](true, false, false, true)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(com.twitter.algebird.Moments(1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(com.twitter.algebird.Moments(2))

    val t3 = obj.prepare(cell3)
    t3 shouldBe Option(com.twitter.algebird.Moments(3))

    val t4 = obj.prepare(cell4)
    t4 shouldBe Option(com.twitter.algebird.Moments(4))

    val t5 = obj.prepare(cell5)
    t5.map(_.mean.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe com.twitter.algebird.Moments(3, 2.0, 2.0, 0.0, 2.0)

    val r3 = obj.reduce(r2, t4.get)
    r3 shouldBe com.twitter.algebird.Moments(4, 2.5, 5.0, 0.0, 10.25)

    val r4 = obj.reduce(r3, t5.get)
    r4 shouldBe com.twitter.algebird.Moments(4, 2.5, 5.0, 0.0, 10.25)

    val c = obj.present(Position("foo"), r4).result
    c shouldBe Option(Cell(Position("foo"), getDoubleContent(-1.36)))
  }

  it should "prepare, reduce and present with non-strict and non-nan" in {
    val obj = Kurtosis[P, S](false, false, false, false)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(com.twitter.algebird.Moments(1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(com.twitter.algebird.Moments(2))

    val t3 = obj.prepare(cell3)
    t3 shouldBe Option(com.twitter.algebird.Moments(3))

    val t4 = obj.prepare(cell4)
    t4 shouldBe Option(com.twitter.algebird.Moments(4))

    val t5 = obj.prepare(cell5)
    t5.map(_.mean.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe com.twitter.algebird.Moments(3, 2.0, 2.0, 0.0, 2.0)

    val r3 = obj.reduce(r2, t4.get)
    r3 shouldBe com.twitter.algebird.Moments(4, 2.5, 5.0, 0.0, 10.25)

    val r4 = obj.reduce(r3, t5.get)
    r4 shouldBe com.twitter.algebird.Moments(4, 2.5, 5.0, 0.0, 10.25)

    val c = obj.present(Position("foo"), r4).result
    c shouldBe Option(Cell(Position("foo"), getDoubleContent(1.64)))
  }

  it should "prepare, reduce and present expanded" in {
    val obj = Kurtosis[P, S]().andThenRelocate(_.position.append("kurtosis").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(com.twitter.algebird.Moments(1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(com.twitter.algebird.Moments(2))

    val t3 = obj.prepare(cell3)
    t3 shouldBe Option(com.twitter.algebird.Moments(3))

    val t4 = obj.prepare(cell4)
    t4 shouldBe Option(com.twitter.algebird.Moments(4))

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe com.twitter.algebird.Moments(3, 2.0, 2.0, 0.0, 2.0)

    val r3 = obj.reduce(r2, t4.get)
    r3 shouldBe com.twitter.algebird.Moments(4, 2.5, 5.0, 0.0, 10.25)

    val c = obj.present(Position("foo"), r3).result
    c shouldBe Option(Cell(Position("foo", "kurtosis"), getDoubleContent(1.64)))
  }

  it should "prepare, reduce and present expanded with strict and nan" in {
    val obj = Kurtosis[P, S](true, false, true, true).andThenRelocate(_.position.append("kurtosis").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(com.twitter.algebird.Moments(1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(com.twitter.algebird.Moments(2))

    val t3 = obj.prepare(cell3)
    t3 shouldBe Option(com.twitter.algebird.Moments(3))

    val t4 = obj.prepare(cell4)
    t4 shouldBe Option(com.twitter.algebird.Moments(4))

    val t5 = obj.prepare(cell5)
    t5.map(_.mean.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe com.twitter.algebird.Moments(3, 2.0, 2.0, 0.0, 2.0)

    val r3 = obj.reduce(r2, t4.get)
    r3 shouldBe com.twitter.algebird.Moments(4, 2.5, 5.0, 0.0, 10.25)

    val r4 = obj.reduce(r3, t5.get)
    r4.mean.compare(Double.NaN) shouldBe 0

    val c = obj.present(Position("foo"), r4).result.get
    c.position shouldBe Position("foo", "kurtosis")
    c.content.value.as[Double].map(_.compare(Double.NaN)) shouldBe Option(0)
  }

  it should "prepare, reduce and present expanded with strict and non-nan" in {
    val obj = Kurtosis[P, S](false, false, true, false).andThenRelocate(_.position.append("kurtosis").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(com.twitter.algebird.Moments(1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(com.twitter.algebird.Moments(2))

    val t3 = obj.prepare(cell3)
    t3 shouldBe Option(com.twitter.algebird.Moments(3))

    val t4 = obj.prepare(cell4)
    t4 shouldBe Option(com.twitter.algebird.Moments(4))

    val t5 = obj.prepare(cell5)
    t5.map(_.mean.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe com.twitter.algebird.Moments(3, 2.0, 2.0, 0.0, 2.0)

    val r3 = obj.reduce(r2, t4.get)
    r3 shouldBe com.twitter.algebird.Moments(4, 2.5, 5.0, 0.0, 10.25)

    val r4 = obj.reduce(r3, t5.get)
    r4.mean.compare(Double.NaN) shouldBe 0

    val c = obj.present(Position("foo"), r4).result
    c shouldBe None
  }

  it should "prepare, reduce and present expanded with non-strict and nan" in {
    val obj = Kurtosis[P, S](true, false, false, true).andThenRelocate(_.position.append("kurtosis").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(com.twitter.algebird.Moments(1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(com.twitter.algebird.Moments(2))

    val t3 = obj.prepare(cell3)
    t3 shouldBe Option(com.twitter.algebird.Moments(3))

    val t4 = obj.prepare(cell4)
    t4 shouldBe Option(com.twitter.algebird.Moments(4))

    val t5 = obj.prepare(cell5)
    t5.map(_.mean.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe com.twitter.algebird.Moments(3, 2.0, 2.0, 0.0, 2.0)

    val r3 = obj.reduce(r2, t4.get)
    r3 shouldBe com.twitter.algebird.Moments(4, 2.5, 5.0, 0.0, 10.25)

    val r4 = obj.reduce(r3, t5.get)
    r4 shouldBe com.twitter.algebird.Moments(4, 2.5, 5.0, 0.0, 10.25)

    val c = obj.present(Position("foo"), r4).result
    c shouldBe Option(Cell(Position("foo", "kurtosis"), getDoubleContent(-1.36)))
  }

  it should "prepare, reduce and present expanded with non-strict and non-nan" in {
    val obj = Kurtosis[P, S](false, false, false, false).andThenRelocate(_.position.append("kurtosis").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(com.twitter.algebird.Moments(1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(com.twitter.algebird.Moments(2))

    val t3 = obj.prepare(cell3)
    t3 shouldBe Option(com.twitter.algebird.Moments(3))

    val t4 = obj.prepare(cell4)
    t4 shouldBe Option(com.twitter.algebird.Moments(4))

    val t5 = obj.prepare(cell5)
    t5.map(_.mean.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe com.twitter.algebird.Moments(3, 2.0, 2.0, 0.0, 2.0)

    val r3 = obj.reduce(r2, t4.get)
    r3 shouldBe com.twitter.algebird.Moments(4, 2.5, 5.0, 0.0, 10.25)

    val r4 = obj.reduce(r3, t5.get)
    r4 shouldBe com.twitter.algebird.Moments(4, 2.5, 5.0, 0.0, 10.25)

    val c = obj.present(Position("foo"), r4).result
    c shouldBe Option(Cell(Position("foo", "kurtosis"), getDoubleContent(1.64)))
  }

  it should "filter" in {
    Kurtosis[P, S](filter=true).prepare(cell1) shouldBe Option(com.twitter.algebird.Moments(1))
    Kurtosis[P, S](filter=false).prepare(cell1) shouldBe Option(com.twitter.algebird.Moments(1))
    Kurtosis[P, S](filter=true).prepare(cell5) shouldBe None
    Kurtosis[P, S](filter=false).prepare(cell5).map(_.mean.compare(Double.NaN)) shouldBe Option(0)
  }
}

class TestMin extends TestAggregators {
  val cell1 = Cell(Position("foo", "one"), getDoubleContent(1))
  val cell2 = Cell(Position("foo", "two"), getDoubleContent(2))
  val cell3 = Cell(Position("foo", "bar"), getStringContent("bar"))

  "A Min" should "prepare, reduce and present" in {
    val obj = Minimum[P, S]()

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(2)

    val r = obj.reduce(t1.get, t2.get)
    r shouldBe 1

    val c = obj.present(Position("foo"), r).result
    c shouldBe Option(Cell(Position("foo"), getDoubleContent(1)))
  }

  it should "prepare, reduce and present with strict and nan" in {
    val obj = Minimum[P, S](false, true, true)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(2)

    val t3 = obj.prepare(cell3)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 1

    val r2 = obj.reduce(r1, t3.get)
    r2.compare(Double.NaN) shouldBe 0

    val c = obj.present(Position("foo"), r2).result.get
    c.position shouldBe Position("foo")
    c.content.value.as[Double].map(_.compare(Double.NaN)) shouldBe Option(0)
  }

  it should "prepare, reduce and present with strict and non-nan" in {
    val obj = Minimum[P, S](false, true, false)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(2)

    val t3 = obj.prepare(cell3)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 1

    val r2 = obj.reduce(r1, t3.get)
    r2.compare(Double.NaN) shouldBe 0

    val c = obj.present(Position("foo"), r2).result
    c shouldBe None
  }

  it should "prepare, reduce and present with non-strict and nan" in {
    val obj = Minimum[P, S](false, false, true)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(2)

    val t3 = obj.prepare(cell3)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 1

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe 1

    val c = obj.present(Position("foo"), r2).result
    c shouldBe Option(Cell(Position("foo"), getDoubleContent(1)))
  }

  it should "prepare, reduce and present with non-strict and non-nan" in {
    val obj = Minimum[P, S](false, false, false)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(2)

    val t3 = obj.prepare(cell3)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 1

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe 1

    val c = obj.present(Position("foo"), r2).result
    c shouldBe Option(Cell(Position("foo"), getDoubleContent(1)))
  }

  it should "prepare, reduce and present expanded" in {
    val obj = Minimum[P, S]().andThenRelocate(_.position.append("min").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(2)

    val r = obj.reduce(t1.get, t2.get)
    r shouldBe 1

    val c = obj.present(Position("foo"), r).result
    c shouldBe Option(Cell(Position("foo", "min"), getDoubleContent(1)))
  }

  it should "prepare, reduce and present expanded with strict and nan" in {
    val obj = Minimum[P, S](false, true, true).andThenRelocate(_.position.append("min").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(2)

    val t3 = obj.prepare(cell3)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 1

    val r2 = obj.reduce(r1, t3.get)
    r2.compare(Double.NaN) shouldBe 0

    val c = obj.present(Position("foo"), r2).result.get
    c.position shouldBe Position("foo", "min")
    c.content.value.as[Double].map(_.compare(Double.NaN)) shouldBe Option(0)
  }

  it should "prepare, reduce and present expanded with strict and non-nan" in {
    val obj = Minimum[P, S](false, true, false).andThenRelocate(_.position.append("min").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(2)

    val t3 = obj.prepare(cell3)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 1

    val r2 = obj.reduce(r1, t3.get)
    r2.compare(Double.NaN) shouldBe 0

    val c = obj.present(Position("foo"), r2).result
    c shouldBe None
  }

  it should "prepare, reduce and present expanded with non-strict and nan" in {
    val obj = Minimum[P, S](false, false, true).andThenRelocate(_.position.append("min").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(2)

    val t3 = obj.prepare(cell3)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 1

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe 1

    val c = obj.present(Position("foo"), r2).result
    c shouldBe Option(Cell(Position("foo", "min"), getDoubleContent(1)))
  }

  it should "prepare, reduce and present expanded with non-strict and non-nan" in {
    val obj = Minimum[P, S](false, false, false).andThenRelocate(_.position.append("min").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(2)

    val t3 = obj.prepare(cell3)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 1

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe 1

    val c = obj.present(Position("foo"), r2).result
    c shouldBe Option(Cell(Position("foo", "min"), getDoubleContent(1)))
  }

  it should "filter" in {
    Minimum[P, S](true).prepare(cell1) shouldBe Option(1)
    Minimum[P, S](false).prepare(cell1) shouldBe Option(1)
    Minimum[P, S](true).prepare(cell3) shouldBe None
    Minimum[P, S](false).prepare(cell3).map(_.compare(Double.NaN)) shouldBe Option(0)
  }
}

class TestMax extends TestAggregators {
  val cell1 = Cell(Position("foo", "one"), getDoubleContent(1))
  val cell2 = Cell(Position("foo", "two"), getDoubleContent(2))
  val cell3 = Cell(Position("foo", "bar"), getStringContent("bar"))

  "A Max" should "prepare, reduce and present" in {
    val obj = Maximum[P, S]()

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(2)

    val r = obj.reduce(t1.get, t2.get)
    r shouldBe 2

    val c = obj.present(Position("foo"), r).result
    c shouldBe Option(Cell(Position("foo"), getDoubleContent(2)))
  }

  it should "prepare, reduce and present with strict and nan" in {
    val obj = Maximum[P, S](false, true, true)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(2)

    val t3 = obj.prepare(cell3)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 2

    val r2 = obj.reduce(r1, t3.get)
    r2.compare(Double.NaN) shouldBe 0

    val c = obj.present(Position("foo"), r2).result.get
    c.position shouldBe Position("foo")
    c.content.value.as[Double].map(_.compare(Double.NaN)) shouldBe Option(0)
  }

  it should "prepare, reduce and present with strict and non-nan" in {
    val obj = Maximum[P, S](false, true, false)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(2)

    val t3 = obj.prepare(cell3)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 2

    val r2 = obj.reduce(r1, t3.get)
    r2.compare(Double.NaN) shouldBe 0

    val c = obj.present(Position("foo"), r2).result
    c shouldBe None
  }

  it should "prepare, reduce and present with non-strict and nan" in {
    val obj = Maximum[P, S](false, false, true)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(2)

    val t3 = obj.prepare(cell3)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 2

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe 2

    val c = obj.present(Position("foo"), r2).result
    c shouldBe Option(Cell(Position("foo"), getDoubleContent(2)))
  }

  it should "prepare, reduce and present with non-strict and non-nan" in {
    val obj = Maximum[P, S](false, false, false)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(2)

    val t3 = obj.prepare(cell3)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 2

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe 2

    val c = obj.present(Position("foo"), r2).result
    c shouldBe Option(Cell(Position("foo"), getDoubleContent(2)))
  }

  it should "prepare, reduce and present expanded" in {
    val obj = Maximum[P, S]().andThenRelocate(_.position.append("max").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(2)

    val r = obj.reduce(t1.get, t2.get)
    r shouldBe 2

    val c = obj.present(Position("foo"), r).result
    c shouldBe Option(Cell(Position("foo", "max"), getDoubleContent(2)))
  }

  it should "prepare, reduce and present expanded with strict and nan" in {
    val obj = Maximum[P, S](false, true, true).andThenRelocate(_.position.append("max").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(2)

    val t3 = obj.prepare(cell3)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 2

    val r2 = obj.reduce(r1, t3.get)
    r2.compare(Double.NaN) shouldBe 0

    val c = obj.present(Position("foo"), r2).result.get
    c.position shouldBe Position("foo", "max")
    c.content.value.as[Double].map(_.compare(Double.NaN)) shouldBe Option(0)
  }

  it should "prepare, reduce and present expanded with strict and non-nan" in {
    val obj = Maximum[P, S](false, true, false).andThenRelocate(_.position.append("max").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(2)

    val t3 = obj.prepare(cell3)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 2

    val r2 = obj.reduce(r1, t3.get)
    r2.compare(Double.NaN) shouldBe 0

    val c = obj.present(Position("foo"), r2).result
    c shouldBe None
  }

  it should "prepare, reduce and present expanded with non-strict and nan" in {
    val obj = Maximum[P, S](false, false, true).andThenRelocate(_.position.append("max").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(2)

    val t3 = obj.prepare(cell3)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 2

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe 2

    val c = obj.present(Position("foo"), r2).result
    c shouldBe Option(Cell(Position("foo", "max"), getDoubleContent(2)))
  }

  it should "prepare, reduce and present expanded with non-strict and non-nan" in {
    val obj = Maximum[P, S](false, false, false).andThenRelocate(_.position.append("max").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(2)

    val t3 = obj.prepare(cell3)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 2

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe 2

    val c = obj.present(Position("foo"), r2).result
    c shouldBe Option(Cell(Position("foo", "max"), getDoubleContent(2)))
  }

  it should "filter" in {
    Maximum[P, S](true).prepare(cell1) shouldBe Option(1)
    Maximum[P, S](false).prepare(cell1) shouldBe Option(1)
    Maximum[P, S](true).prepare(cell3) shouldBe None
    Maximum[P, S](false).prepare(cell3).map(_.compare(Double.NaN)) shouldBe Option(0)
  }
}

class TestMaximumAbsolute extends TestAggregators {
  val cell1 = Cell(Position("foo", "one"), getDoubleContent(1))
  val cell2 = Cell(Position("foo", "two"), getDoubleContent(-2))
  val cell3 = Cell(Position("foo", "bar"), getStringContent("bar"))

  "A MaximumAbsolute" should "prepare, reduce and present" in {
    val obj = MaximumAbsolute[P, S]()

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(-2)

    val r = obj.reduce(t1.get, t2.get)
    r shouldBe 2

    val c = obj.present(Position("foo"), r).result
    c shouldBe Option(Cell(Position("foo"), getDoubleContent(2)))
  }

  it should "prepare, reduce and present with strict and nan" in {
    val obj = MaximumAbsolute[P, S](false, true, true)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(-2)

    val t3 = obj.prepare(cell3)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 2

    val r2 = obj.reduce(r1, t3.get)
    r2.compare(Double.NaN) shouldBe 0

    val c = obj.present(Position("foo"), r2).result.get
    c.position shouldBe Position("foo")
    c.content.value.as[Double].map(_.compare(Double.NaN)) shouldBe Option(0)
  }

  it should "prepare, reduce and present with strict and non-nan" in {
    val obj = MaximumAbsolute[P, S](false, true, false)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(-2)

    val t3 = obj.prepare(cell3)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 2

    val r2 = obj.reduce(r1, t3.get)
    r2.compare(Double.NaN) shouldBe 0

    val c = obj.present(Position("foo"), r2).result
    c shouldBe None
  }

  it should "prepare, reduce and present with non-strict and nan" in {
    val obj = MaximumAbsolute[P, S](false, false, true)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(-2)

    val t3 = obj.prepare(cell3)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 2

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe 2

    val c = obj.present(Position("foo"), r2).result
    c shouldBe Option(Cell(Position("foo"), getDoubleContent(2)))
  }

  it should "prepare, reduce and present with non-strict and non-nan" in {
    val obj = MaximumAbsolute[P, S](false, false, false)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(-2)

    val t3 = obj.prepare(cell3)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 2

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe 2

    val c = obj.present(Position("foo"), r2).result
    c shouldBe Option(Cell(Position("foo"), getDoubleContent(2)))
  }

  it should "prepare, reduce and present expanded" in {
    val obj = MaximumAbsolute[P, S]().andThenRelocate(_.position.append("max.abs").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(-2)

    val r = obj.reduce(t1.get, t2.get)
    r shouldBe 2

    val c = obj.present(Position("foo"), r).result
    c shouldBe Option(Cell(Position("foo", "max.abs"), getDoubleContent(2)))
  }

  it should "prepare, reduce and present expanded with strict and nan" in {
    val obj = MaximumAbsolute[P, S](false, true, true).andThenRelocate(_.position.append("max.abs").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(-2)

    val t3 = obj.prepare(cell3)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 2

    val r2 = obj.reduce(r1, t3.get)
    r2.compare(Double.NaN) shouldBe 0

    val c = obj.present(Position("foo"), r2).result.get
    c.position shouldBe Position("foo", "max.abs")
    c.content.value.as[Double].map(_.compare(Double.NaN)) shouldBe Option(0)
  }

  it should "prepare, reduce and present expanded with strict and non-nan" in {
    val obj = MaximumAbsolute[P, S](false, true, false).andThenRelocate(_.position.append("max.abs").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(-2)

    val t3 = obj.prepare(cell3)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 2

    val r2 = obj.reduce(r1, t3.get)
    r2.compare(Double.NaN) shouldBe 0

    val c = obj.present(Position("foo"), r2).result
    c shouldBe None
  }

  it should "prepare, reduce and present expanded with non-strict and nan" in {
    val obj = MaximumAbsolute[P, S](false, false, true).andThenRelocate(_.position.append("max.abs").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(-2)

    val t3 = obj.prepare(cell3)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 2

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe 2

    val c = obj.present(Position("foo"), r2).result
    c shouldBe Option(Cell(Position("foo", "max.abs"), getDoubleContent(2)))
  }

  it should "prepare, reduce and present expanded with non-strict and non-nan" in {
    val obj = MaximumAbsolute[P, S](false, false, false).andThenRelocate(_.position.append("max.abs").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(-2)

    val t3 = obj.prepare(cell3)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 2

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe 2

    val c = obj.present(Position("foo"), r2).result
    c shouldBe Option(Cell(Position("foo", "max.abs"), getDoubleContent(2)))
  }

  it should "filter" in {
    MaximumAbsolute[P, S](true).prepare(cell1) shouldBe Option(1)
    MaximumAbsolute[P, S](false).prepare(cell1) shouldBe Option(1)
    MaximumAbsolute[P, S](true).prepare(cell3) shouldBe None
    MaximumAbsolute[P, S](false).prepare(cell3).map(_.compare(Double.NaN)) shouldBe Option(0)
  }
}

class TestSums extends TestAggregators {
  val cell1 = Cell(Position("foo", "one"), getDoubleContent(1))
  val cell2 = Cell(Position("foo", "two"), getDoubleContent(2))
  val cell3 = Cell(Position("foo", "bar"), getStringContent("bar"))

  "A Sums" should "prepare, reduce and present" in {
    val obj = Sums[P, S]()

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(2)

    val r = obj.reduce(t1.get, t2.get)
    r shouldBe 3

    val c = obj.present(Position("foo"), r).result
    c shouldBe Option(Cell(Position("foo"), getDoubleContent(3)))
  }

  it should "prepare, reduce and present with strict and nan" in {
    val obj = Sums[P, S](false, true, true)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(2)

    val t3 = obj.prepare(cell3)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 3

    val r2 = obj.reduce(r1, t3.get)
    r2.compare(Double.NaN) shouldBe 0

    val c = obj.present(Position("foo"), r2).result.get
    c.position shouldBe Position("foo")
    c.content.value.as[Double].map(_.compare(Double.NaN)) shouldBe Option(0)
  }

  it should "prepare, reduce and present with strict and non-nan" in {
    val obj = Sums[P, S](false, true, false)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(2)

    val t3 = obj.prepare(cell3)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 3

    val r2 = obj.reduce(r1, t3.get)
    r2.compare(Double.NaN) shouldBe 0

    val c = obj.present(Position("foo"), r2).result
    c shouldBe None
  }

  it should "prepare, reduce and present with non-strict and nan" in {
    val obj = Sums[P, S](false, false, true)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(2)

    val t3 = obj.prepare(cell3)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 3

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe 3

    val c = obj.present(Position("foo"), r2).result
    c shouldBe Option(Cell(Position("foo"), getDoubleContent(3)))
  }

  it should "prepare, reduce and present with non-strict and non-nan" in {
    val obj = Sums[P, S](false, false, false)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(2)

    val t3 = obj.prepare(cell3)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 3

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe 3

    val c = obj.present(Position("foo"), r2).result
    c shouldBe Option(Cell(Position("foo"), getDoubleContent(3)))
  }

  it should "prepare, reduce and present expanded" in {
    val obj = Sums[P, S]().andThenRelocate(_.position.append("sum").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(2)

    val r = obj.reduce(t1.get, t2.get)
    r shouldBe 3

    val c = obj.present(Position("foo"), r).result
    c shouldBe Option(Cell(Position("foo", "sum"), getDoubleContent(3)))
  }

  it should "prepare, reduce and present expanded with strict and nan" in {
    val obj = Sums[P, S](false, true, true).andThenRelocate(_.position.append("sum").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(2)

    val t3 = obj.prepare(cell3)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 3

    val r2 = obj.reduce(r1, t3.get)
    r2.compare(Double.NaN) shouldBe 0

    val c = obj.present(Position("foo"), r2).result.get
    c.position shouldBe Position("foo", "sum")
    c.content.value.as[Double].map(_.compare(Double.NaN)) shouldBe Option(0)
  }

  it should "prepare, reduce and present expanded with strict and non-nan" in {
    val obj = Sums[P, S](false, true, false).andThenRelocate(_.position.append("sum").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(2)

    val t3 = obj.prepare(cell3)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 3

    val r2 = obj.reduce(r1, t3.get)
    r2.compare(Double.NaN) shouldBe 0

    val c = obj.present(Position("foo"), r2).result
    c shouldBe None
  }

  it should "prepare, reduce and present expanded with non-strict and nan" in {
    val obj = Sums[P, S](false, false, true).andThenRelocate(_.position.append("sum").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(2)

    val t3 = obj.prepare(cell3)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 3

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe 3

    val c = obj.present(Position("foo"), r2).result
    c shouldBe Option(Cell(Position("foo", "sum"), getDoubleContent(3)))
  }

  it should "prepare, reduce and present expanded with non-strict and non-nan" in {
    val obj = Sums[P, S](false, false, false).andThenRelocate(_.position.append("sum").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(2)

    val t3 = obj.prepare(cell3)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 3

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe 3

    val c = obj.present(Position("foo"), r2).result
    c shouldBe Option(Cell(Position("foo", "sum"), getDoubleContent(3)))
  }

  it should "filter" in {
    Sums[P, S](true).prepare(cell1) shouldBe Option(1)
    Sums[P, S](false).prepare(cell1) shouldBe Option(1)
    Sums[P, S](true).prepare(cell3) shouldBe None
    Sums[P, S](false).prepare(cell3).map(_.compare(Double.NaN)) shouldBe Option(0)
  }
}

class TestPredicateCounts extends TestAggregators {
  val cell1 = Cell(Position("foo", "one"), getDoubleContent(-1))
  val cell2 = Cell(Position("foo", "two"), getDoubleContent(0))
  val cell3 = Cell(Position("foo", "three"), getDoubleContent(1))
  val cell4 = Cell(Position("foo", "bar"), getStringContent("bar"))

  def predicate(con: Content) = con.value.as[Double].map(_ <= 0).getOrElse(false)

  "A PredicateCounts" should "prepare, reduce and present" in {
    val obj = PredicateCounts[P, S](predicate)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(1)

    val t3 = obj.prepare(cell3)
    t3 shouldBe None

    val t4 = obj.prepare(cell4)
    t4 shouldBe None

    val r = obj.reduce(t1.get, t2.get)
    r shouldBe 2

    val c = obj.present(Position("foo"), r).result
    c shouldBe Option(Cell(Position("foo"), getLongContent(2)))
  }
}

class TestWeightedSums extends TestAggregators {
  type Q = Value[String] :: Value[Int] :: HNil

  val cell1 = Cell(Position("foo", 1), getDoubleContent(-1))
  val cell2 = Cell(Position("bar", 2), getDoubleContent(1))
  val cell3 = Cell(Position("xyz", 3), getStringContent("abc"))
  val ext = Map(
    Position("foo") -> 3.14,
    Position("bar") -> 6.28,
    Position("foo.model1") -> 3.14,
    Position("bar.model1") -> 6.28,
    Position("2.model2") -> -3.14
  )
  val ext2 = Map(
    Position(2) -> 3.14
  )

  type W = Map[Position[Value[String] :: HNil], Double]
  type X = Map[Position[Value[Int] :: HNil], Double]

  def extractor1 = ExtractWithDimension[Q, _0, Double]
  def extractor2 = ExtractWithDimension[Q, _1, Double]

  case class ExtractWithName[
    D <: Nat
  ](
    dim: D,
    name: String
  )(implicit
    ev: Position.IndexConstraints[Q, D] { type V <: Value[_] }
  ) extends Extract[Q, W, Double] {
    def extract(cell: Cell[Q], ext: W): Option[Double] = ext
      .get(Position(name.format(cell.position(dim).toShortString)))
  }

  "A WeightedSums" should "prepare, reduce and present on the first dimension" in {
    val obj = WeightedSums[Q, S, W](extractor1)

    val t1 = obj.prepareWithValue(cell1, ext)
    t1 shouldBe Option(-3.14)

    val t2 = obj.prepareWithValue(cell2, ext)
    t2 shouldBe Option(6.28)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 3.14

    val c = obj.presentWithValue(Position("foo"), r1, ext).result
    c shouldBe Option(Cell(Position("foo"), getDoubleContent(3.14)))
  }

  it should "prepare, reduce and present on the second dimension" in {
    val obj = WeightedSums[Q, S, X](extractor2)

    val t1 = obj.prepareWithValue(cell1, ext2)
    t1 shouldBe Option(0)

    val t2 = obj.prepareWithValue(cell2, ext2)
    t2 shouldBe Option(3.14)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 3.14

    val c = obj.presentWithValue(Position("foo"), r1, ext2).result
    c shouldBe Option(Cell(Position("foo"), getDoubleContent(3.14)))
  }

  it should "prepare, reduce and present with strict and nan" in {
    val obj = WeightedSums[Q, S, W](extractor1, false, true, true)

    val t1 = obj.prepareWithValue(cell1, ext)
    t1 shouldBe Option(-3.14)

    val t2 = obj.prepareWithValue(cell2, ext)
    t2 shouldBe Option(6.28)

    val t3 = obj.prepareWithValue(cell3, ext)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 3.14

    val r2 = obj.reduce(r1, t3.get)
    r2.compare(Double.NaN) shouldBe 0

    val c = obj.presentWithValue(Position("foo"), r2, ext).result.get
    c.position shouldBe Position("foo")
    c.content.value.as[Double].map(_.compare(Double.NaN)) shouldBe Option(0)
  }

  it should "prepare, reduce and present with strict and non-nan" in {
    val obj = WeightedSums[Q, S, W](extractor1, false, true, false)

    val t1 = obj.prepareWithValue(cell1, ext)
    t1 shouldBe Option(-3.14)

    val t2 = obj.prepareWithValue(cell2, ext)
    t2 shouldBe Option(6.28)

    val t3 = obj.prepareWithValue(cell3, ext)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 3.14

    val r2 = obj.reduce(r1, t3.get)
    r2.compare(Double.NaN) shouldBe 0

    val c = obj.presentWithValue(Position("foo"), r2, ext).result
    c shouldBe None
  }

  it should "prepare, reduce and present with non-strict and nan" in {
    val obj = WeightedSums[Q, S, W](extractor1, false, false, true)

    val t1 = obj.prepareWithValue(cell1, ext)
    t1 shouldBe Option(-3.14)

    val t2 = obj.prepareWithValue(cell2, ext)
    t2 shouldBe Option(6.28)

    val t3 = obj.prepareWithValue(cell3, ext)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 3.14

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe 3.14

    val c = obj.presentWithValue(Position("foo"), r2, ext).result
    c shouldBe Option(Cell(Position("foo"), getDoubleContent(3.14)))
  }

  it should "prepare, reduce and present with non-strict and non-nan" in {
    val obj = WeightedSums[Q, S, W](extractor1, false, false, false)

    val t1 = obj.prepareWithValue(cell1, ext)
    t1 shouldBe Option(-3.14)

    val t2 = obj.prepareWithValue(cell2, ext)
    t2 shouldBe Option(6.28)

    val t3 = obj.prepareWithValue(cell3, ext)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 3.14

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe 3.14

    val c = obj.presentWithValue(Position("foo"), r2, ext).result
    c shouldBe Option(Cell(Position("foo"), getDoubleContent(3.14)))
  }

  it should "prepare, reduce and present expanded on the first dimension" in {
    val obj = WeightedSums[Q, S, W](extractor1)
      .andThenRelocateWithValue((c: Cell[S], e: W) => c.position.append("result").toOption)

    val t1 = obj.prepareWithValue(cell1, ext)
    t1 shouldBe Option(-3.14)

    val t2 = obj.prepareWithValue(cell2, ext)
    t2 shouldBe Option(6.28)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 3.14

    val c = obj.presentWithValue(Position("foo"), r1, ext).result
    c shouldBe Option(Cell(Position("foo", "result"), getDoubleContent(3.14)))
  }

  it should "prepare, reduce and present expanded on the second dimension" in {
    val obj = WeightedSums[Q, S, X](extractor2)
      .andThenRelocateWithValue((c: Cell[S], e: X) => c.position.append("result").toOption)

    val t1 = obj.prepareWithValue(cell1, ext2)
    t1 shouldBe Option(0)

    val t2 = obj.prepareWithValue(cell2, ext2)
    t2 shouldBe Option(3.14)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 3.14

    val c = obj.presentWithValue(Position("foo"), r1, ext2).result
    c shouldBe Option(Cell(Position("foo", "result"), getDoubleContent(3.14)))
  }

  it should "prepare, reduce and present expanded with strict and nan" in {
    val obj = WeightedSums[Q, S, W](extractor1, false, true, true)
      .andThenRelocateWithValue((c: Cell[S], e: W) => c.position.append("result").toOption)

    val t1 = obj.prepareWithValue(cell1, ext)
    t1 shouldBe Option(-3.14)

    val t2 = obj.prepareWithValue(cell2, ext)
    t2 shouldBe Option(6.28)

    val t3 = obj.prepareWithValue(cell3, ext)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 3.14

    val r2 = obj.reduce(r1, t3.get)
    r2.compare(Double.NaN) shouldBe 0

    val c = obj.presentWithValue(Position("foo"), r2, ext).result.get
    c.position shouldBe Position("foo", "result")
    c.content.value.as[Double].map(_.compare(Double.NaN)) shouldBe Option(0)
  }

  it should "prepare, reduce and present expanded with strict and non-nan" in {
    val obj = WeightedSums[Q, S, W](extractor1, false, true, false)
      .andThenRelocateWithValue((c: Cell[S], e: W) => c.position.append("result").toOption)

    val t1 = obj.prepareWithValue(cell1, ext)
    t1 shouldBe Option(-3.14)

    val t2 = obj.prepareWithValue(cell2, ext)
    t2 shouldBe Option(6.28)

    val t3 = obj.prepareWithValue(cell3, ext)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 3.14

    val r2 = obj.reduce(r1, t3.get)
    r2.compare(Double.NaN) shouldBe 0

    val c = obj.presentWithValue(Position("foo"), r2, ext).result
    c shouldBe None
  }

  it should "prepare, reduce and present expanded with non-strict and nan" in {
    val obj = WeightedSums[Q, S, W](extractor1, false, false, true)
      .andThenRelocateWithValue((c: Cell[S], e: W) => c.position.append("result").toOption)

    val t1 = obj.prepareWithValue(cell1, ext)
    t1 shouldBe Option(-3.14)

    val t2 = obj.prepareWithValue(cell2, ext)
    t2 shouldBe Option(6.28)

    val t3 = obj.prepareWithValue(cell3, ext)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 3.14

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe 3.14

    val c = obj.presentWithValue(Position("foo"), r2, ext).result
    c shouldBe Option(Cell(Position("foo", "result"), getDoubleContent(3.14)))
  }

  it should "prepare, reduce and present expanded with non-strict and non-nan" in {
    val obj = WeightedSums[Q, S, W](extractor1, false, false, false)
      .andThenRelocateWithValue((c: Cell[S], e: W) => c.position.append("result").toOption)

    val t1 = obj.prepareWithValue(cell1, ext)
    t1 shouldBe Option(-3.14)

    val t2 = obj.prepareWithValue(cell2, ext)
    t2 shouldBe Option(6.28)

    val t3 = obj.prepareWithValue(cell3, ext)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 3.14

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe 3.14

    val c = obj.presentWithValue(Position("foo"), r2, ext).result
    c shouldBe Option(Cell(Position("foo", "result"), getDoubleContent(3.14)))
  }

  it should "prepare, reduce and present multiple on the first dimension with format" in {
    val obj = WeightedSums[Q, S, W](ExtractWithName(_0, "%1$s.model1"))
      .andThenRelocateWithValue((c: Cell[S], e: W) => c.position.append("result").toOption)

    val t1 = obj.prepareWithValue(cell1, ext)
    t1 shouldBe Option(-3.14)

    val t2 = obj.prepareWithValue(cell2, ext)
    t2 shouldBe Option(6.28)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 3.14

    val c = obj.presentWithValue(Position("foo"), r1, ext).result
    c shouldBe Option(Cell(Position("foo", "result"), getDoubleContent(3.14)))
  }

  it should "prepare, reduce and present multiple on the second dimension with format" in {
    val obj = WeightedSums[Q, S, W](ExtractWithName(_1, "%1$s.model2"))
      .andThenRelocateWithValue((c: Cell[S], e: W) => c.position.append("result").toOption)

    val t1 = obj.prepareWithValue(cell1, ext)
    t1 shouldBe Option(0)

    val t2 = obj.prepareWithValue(cell2, ext)
    t2 shouldBe Option(-3.14)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe -3.14

    val c = obj.presentWithValue(Position("foo"), r1, ext).result
    c shouldBe Option(Cell(Position("foo", "result"), getDoubleContent(-3.14)))
  }

  it should "prepare, reduce and present multiple with strict and nan with format" in {
    val obj = WeightedSums[Q, S, W](ExtractWithName(_0, "%1$s.model1"), false, true, true)
      .andThenRelocateWithValue((c: Cell[S], e: W) => c.position.append("result").toOption)

    val t1 = obj.prepareWithValue(cell1, ext)
    t1 shouldBe Option(-3.14)

    val t2 = obj.prepareWithValue(cell2, ext)
    t2 shouldBe Option(6.28)

    val t3 = obj.prepareWithValue(cell3, ext)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 3.14

    val r2 = obj.reduce(r1, t3.get)
    r2.compare(Double.NaN) shouldBe 0

    val c = obj.presentWithValue(Position("foo"), r2, ext).result.get
    c.position shouldBe Position("foo", "result")
    c.content.value.as[Double].map(_.compare(Double.NaN)) shouldBe Option(0)
  }

  it should "prepare, reduce and present multiple with strict and non-nan with format" in {
    val obj = WeightedSums[Q, S, W](ExtractWithName(_0, "%1$s.model1"), false, true, false)
      .andThenRelocateWithValue((c: Cell[S], e: W) => c.position.append("result").toOption)

    val t1 = obj.prepareWithValue(cell1, ext)
    t1 shouldBe Option(-3.14)

    val t2 = obj.prepareWithValue(cell2, ext)
    t2 shouldBe Option(6.28)

    val t3 = obj.prepareWithValue(cell3, ext)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 3.14

    val r2 = obj.reduce(r1, t3.get)
    r2.compare(Double.NaN) shouldBe 0

    val c = obj.presentWithValue(Position("foo"), r2, ext).result
    c shouldBe None
  }

  it should "prepare, reduce and present multiple with non-strict and nan with format" in {
    val obj = WeightedSums[Q, S, W](ExtractWithName(_0, "%1$s.model1"), false, false, true)
      .andThenRelocateWithValue((c: Cell[S], e: W) => c.position.append("result").toOption)

    val t1 = obj.prepareWithValue(cell1, ext)
    t1 shouldBe Option(-3.14)

    val t2 = obj.prepareWithValue(cell2, ext)
    t2 shouldBe Option(6.28)

    val t3 = obj.prepareWithValue(cell3, ext)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 3.14

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe 3.14

    val c = obj.presentWithValue(Position("foo"), r2, ext).result
    c shouldBe Option(Cell(Position("foo", "result"), getDoubleContent(3.14)))
  }

  it should "prepare, reduce and present multiple with non-strict and non-nan with format" in {
    val obj = WeightedSums[Q, S, W](ExtractWithName(_0, "%1$s.model1"), false, false, false)
      .andThenRelocateWithValue((c: Cell[S], e: W) => c.position.append("result").toOption)

    val t1 = obj.prepareWithValue(cell1, ext)
    t1 shouldBe Option(-3.14)

    val t2 = obj.prepareWithValue(cell2, ext)
    t2 shouldBe Option(6.28)

    val t3 = obj.prepareWithValue(cell3, ext)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 3.14

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe 3.14

    val c = obj.presentWithValue(Position("foo"), r2, ext).result
    c shouldBe Option(Cell(Position("foo", "result"), getDoubleContent(3.14)))
  }

  it should "filter" in {
    WeightedSums[Q, S, W](extractor1, true).prepareWithValue(cell1, ext) shouldBe Option(-3.14)
    WeightedSums[Q, S, W](extractor1, false).prepareWithValue(cell1, ext) shouldBe Option(-3.14)
    WeightedSums[Q, S, W](extractor1, true).prepareWithValue(cell3, ext) shouldBe None
    WeightedSums[Q, S, W](extractor1, false).prepareWithValue(cell3, ext).map(_.compare(Double.NaN)) shouldBe Option(0)
  }
}

class TestDistinctCounts extends TestAggregators {
  type Q = Value[String] :: Value[Int] :: HNil

  val cell1 = Cell(Position("foo", 1), getDoubleContent(1))
  val cell2 = Cell(Position("foo", 2), getDoubleContent(1))
  val cell3 = Cell(Position("foo", 3), getDoubleContent(1))
  val cell4 = Cell(Position("abc", 4), getStringContent("abc"))
  val cell5 = Cell(Position("xyz", 4), getStringContent("abc"))
  val cell6 = Cell(Position("bar", 5), getLongContent(123))

  "A DistinctCounts" should "prepare, reduce and present" in {
    val obj = DistinctCounts[Q, S]()

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(Set(DoubleValue(1)))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(Set(DoubleValue(1)))

    val t3 = obj.prepare(cell3)
    t3 shouldBe Option(Set(DoubleValue(1)))

    val t4 = obj.prepare(cell4)
    t4 shouldBe Option(Set(StringValue("abc")))

    val t5 = obj.prepare(cell5)
    t5 shouldBe Option(Set(StringValue("abc")))

    val t6 = obj.prepare(cell6)
    t6 shouldBe Option(Set(LongValue(123)))

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe Set(DoubleValue(1))

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe Set(DoubleValue(1))

    val r3 = obj.reduce(r2, t4.get)
    r3 shouldBe Set(DoubleValue(1), StringValue("abc"))

    val r4 = obj.reduce(r3, t5.get)
    r4 shouldBe Set(DoubleValue(1), StringValue("abc"))

    val r5 = obj.reduce(r4, t6.get)
    r5 shouldBe Set(DoubleValue(1), StringValue("abc"), LongValue(123))

    val c = obj.present(Position("foo"), r5).result
    c shouldBe Option(Cell(Position("foo"), getLongContent(3)))
  }

  it should "prepare, reduce and present expanded" in {
    val obj = DistinctCounts[Q, S]().andThenRelocate(_.position.append("count").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(Set(DoubleValue(1)))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(Set(DoubleValue(1)))

    val t3 = obj.prepare(cell3)
    t3 shouldBe Option(Set(DoubleValue(1)))

    val t4 = obj.prepare(cell4)
    t4 shouldBe Option(Set(StringValue("abc")))

    val t5 = obj.prepare(cell5)
    t5 shouldBe Option(Set(StringValue("abc")))

    val t6 = obj.prepare(cell6)
    t6 shouldBe Option(Set(LongValue(123)))

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe Set(DoubleValue(1))

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe Set(DoubleValue(1))

    val r3 = obj.reduce(r2, t4.get)
    r3 shouldBe Set(DoubleValue(1), StringValue("abc"))

    val r4 = obj.reduce(r3, t5.get)
    r4 shouldBe Set(DoubleValue(1), StringValue("abc"))

    val r5 = obj.reduce(r4, t6.get)
    r5 shouldBe Set(DoubleValue(1), StringValue("abc"), LongValue(123))

    val c = obj.present(Position("foo"), r5).result
    c shouldBe Option(Cell(Position("foo", "count"), getLongContent(3)))
  }
}

class TestEntropy extends TestAggregators {
  val cell1 = Cell(Position("foo", "abc"), getLongContent(1))
  val cell2 = Cell(Position("foo", "xyz"), getLongContent(2))
  val cell3 = Cell(Position("foo", "123"), getStringContent("456"))

  def log2(x: Double) = math.log(x) / math.log(2)
  def log4(x: Double) = math.log(x) / math.log(4)

  def extractor = ExtractWithDimension[P, _0, Double]

  val count = Map(Position("foo") -> 3.0)

  type W = Map[Position[S], Double]

  "An Entropy" should "prepare, reduce and present" in {
    val obj = Entropy[P, S, W](extractor, false)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe Option(((1, 1.0/3 * log2(1.0/3))))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe Option(((1, 2.0/3 * log2(2.0/3))))

    val t3 = obj.prepareWithValue(cell3, count)
    t3.get._1 shouldBe 1
    t3.get._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe ((2, (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))

    val r2 = obj.reduce(r1, t3.get)
    r2._1 shouldBe 3
    r2._2.compare(Double.NaN) shouldBe 0

    val c = obj.presentWithValue(Position("foo"), r2, count).result
    c shouldBe None
  }

  it should "prepare, reduce and present with strict, nan negate" in {
    val obj = Entropy[P, S, W](extractor, false, true, true, true)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe Option(((1, 1.0/3 * log2(1.0/3))))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe Option(((1, 2.0/3 * log2(2.0/3))))

    val t3 = obj.prepareWithValue(cell3, count)
    t3.get._1 shouldBe 1
    t3.get._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe ((2, (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))

    val r2 = obj.reduce(r1, t3.get)
    r2._1 shouldBe 3
    r2._2.compare(Double.NaN) shouldBe 0

    val c = obj.presentWithValue(Position("foo"), r2, count).result.get
    c.position shouldBe Position("foo")
    c.content.value.as[Double].map(_.compare(Double.NaN)) shouldBe Option(0)
  }

  it should "prepare, reduce and present strict, nan and non-negate" in {
    val obj = Entropy[P, S, W](extractor, false, true, true, false)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe Option(((1, 1.0/3 * log2(1.0/3))))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe Option(((1, 2.0/3 * log2(2.0/3))))

    val t3 = obj.prepareWithValue(cell3, count)
    t3.get._1 shouldBe 1
    t3.get._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe ((2, (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))

    val r2 = obj.reduce(r1, t3.get)
    r2._1 shouldBe 3
    r2._2.compare(Double.NaN) shouldBe 0

    val c = obj.presentWithValue(Position("foo"), r2, count).result.get
    c.position shouldBe Position("foo")
    c.content.value.as[Double].map(_.compare(Double.NaN)) shouldBe Option(0)
  }

  it should "prepare, reduce and present single with strict, non-nan and negate" in {
    val obj = Entropy[P, S, W](extractor, false, true, false, true)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe Option(((1, 1.0/3 * log2(1.0/3))))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe Option(((1, 2.0/3 * log2(2.0/3))))

    val t3 = obj.prepareWithValue(cell3, count)
    t3.get._1 shouldBe 1
    t3.get._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe ((2, (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))

    val r2 = obj.reduce(r1, t3.get)
    r2._1 shouldBe 3
    r2._2.compare(Double.NaN) shouldBe 0

    val c = obj.presentWithValue(Position("foo"), r2, count).result
    c shouldBe None
  }

  it should "prepare, reduce and present with strict, non-nan and non-negate" in {
    val obj = Entropy[P, S, W](extractor, false, true, false, false)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe Option(((1, 1.0/3 * log2(1.0/3))))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe Option(((1, 2.0/3 * log2(2.0/3))))

    val t3 = obj.prepareWithValue(cell3, count)
    t3.get._1 shouldBe 1
    t3.get._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe ((2, (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))

    val r2 = obj.reduce(r1, t3.get)
    r2._1 shouldBe 3
    r2._2.compare(Double.NaN) shouldBe 0

    val c = obj.presentWithValue(Position("foo"), r2, count).result
    c shouldBe None
  }

  it should "prepare, reduce and present with non-strict, nan and negate" in {
    val obj = Entropy[P, S, W](extractor, false, false, true, true)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe Option(((1, 1.0/3 * log2(1.0/3))))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe Option(((1, 2.0/3 * log2(2.0/3))))

    val t3 = obj.prepareWithValue(cell3, count)
    t3.get._1 shouldBe 1
    t3.get._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe ((2, (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe ((3, (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))

    val c = obj.presentWithValue(Position("foo"), r2, count).result
    c shouldBe Option(Cell(Position("foo"), getDoubleContent(2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))
  }

  it should "prepare, reduce and present with non-strict, nan and non-negate" in {
    val obj = Entropy[P, S, W](extractor, false, false, true, false)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe Option(((1, 1.0/3 * log2(1.0/3))))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe Option(((1, 2.0/3 * log2(2.0/3))))

    val t3 = obj.prepareWithValue(cell3, count)
    t3.get._1 shouldBe 1
    t3.get._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe ((2, (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe ((3, (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))

    val c = obj.presentWithValue(Position("foo"), r2, count).result
    c shouldBe Option(Cell(Position("foo"), getDoubleContent(- (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3)))))
  }

  it should "prepare, reduce and present with non-strict, non-nan and negate" in {
    val obj = Entropy[P, S, W](extractor, false, false, false, true)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe Option(((1, 1.0/3 * log2(1.0/3))))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe Option(((1, 2.0/3 * log2(2.0/3))))

    val t3 = obj.prepareWithValue(cell3, count)
    t3.get._1 shouldBe 1
    t3.get._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe ((2, (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe ((3, (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))

    val c = obj.presentWithValue(Position("foo"), r2, count).result
    c shouldBe Option(Cell(Position("foo"), getDoubleContent(2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))
  }

  it should "prepare, reduce and present with non-strict, non-nan and non-negate" in {
    val obj = Entropy[P, S, W](extractor, false, false, false, false)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe Option(((1, 1.0/3 * log2(1.0/3))))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe Option(((1, 2.0/3 * log2(2.0/3))))

    val t3 = obj.prepareWithValue(cell3, count)
    t3.get._1 shouldBe 1
    t3.get._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe ((2, (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe ((3, (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))

    val c = obj.presentWithValue(Position("foo"), r2, count).result
    c shouldBe Option(Cell(Position("foo"), getDoubleContent(- (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3)))))
  }

  it should "prepare, reduce and present with log" in {
    val obj = Entropy[P, S, W](extractor, false, log = log4 _)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe Option(((1, 1.0/3 * log4(1.0/3))))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe Option(((1, 2.0/3 * log4(2.0/3))))

    val t3 = obj.prepareWithValue(cell3, count)
    t3.get._1 shouldBe 1
    t3.get._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe ((2, (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))

    val r2 = obj.reduce(r1, t3.get)
    r2._1 shouldBe 3
    r2._2.compare(Double.NaN) shouldBe 0

    val c = obj.presentWithValue(Position("foo"), r2, count).result
    c shouldBe None
  }

  it should "prepare, reduce and present with strict, nan and negate with log" in {
    val obj = Entropy[P, S, W](extractor, false, true, true, true, log4 _)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe Option(((1, 1.0/3 * log4(1.0/3))))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe Option(((1, 2.0/3 * log4(2.0/3))))

    val t3 = obj.prepareWithValue(cell3, count)
    t3.get._1 shouldBe 1
    t3.get._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe ((2, (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))

    val r2 = obj.reduce(r1, t3.get)
    r2._1 shouldBe 3
    r2._2.compare(Double.NaN) shouldBe 0

    val c = obj.presentWithValue(Position("foo"), r2, count).result.get
    c.position shouldBe Position("foo")
    c.content.value.as[Double].map(_.compare(Double.NaN)) shouldBe Option(0)
  }

  it should "prepare, reduce and present with strict, nan and non-negate with log" in {
    val obj = Entropy[P, S, W](extractor, false, true, true, false, log4 _)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe Option(((1, 1.0/3 * log4(1.0/3))))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe Option(((1, 2.0/3 * log4(2.0/3))))

    val t3 = obj.prepareWithValue(cell3, count)
    t3.get._1 shouldBe 1
    t3.get._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe ((2, (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))

    val r2 = obj.reduce(r1, t3.get)
    r2._1 shouldBe 3
    r2._2.compare(Double.NaN) shouldBe 0

    val c = obj.presentWithValue(Position("foo"), r2, count).result.get
    c.position shouldBe Position("foo")
    c.content.value.as[Double].map(_.compare(Double.NaN)) shouldBe Option(0)
  }

  it should "prepare, reduce and present with strict, non-nan and negate with log" in {
    val obj = Entropy[P, S, W](extractor, false, true, false, true, log4 _)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe Option(((1, 1.0/3 * log4(1.0/3))))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe Option(((1, 2.0/3 * log4(2.0/3))))

    val t3 = obj.prepareWithValue(cell3, count)
    t3.get._1 shouldBe 1
    t3.get._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe ((2, (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))

    val r2 = obj.reduce(r1, t3.get)
    r2._1 shouldBe 3
    r2._2.compare(Double.NaN) shouldBe 0

    val c = obj.presentWithValue(Position("foo"), r2, count).result
    c shouldBe None
  }

  it should "prepare, reduce and present with strict, non-nan and non-negate with log" in {
    val obj = Entropy[P, S, W](extractor, false, true, false, false, log4 _)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe Option(((1, 1.0/3 * log4(1.0/3))))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe Option(((1, 2.0/3 * log4(2.0/3))))

    val t3 = obj.prepareWithValue(cell3, count)
    t3.get._1 shouldBe 1
    t3.get._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe ((2, (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))

    val r2 = obj.reduce(r1, t3.get)
    r2._1 shouldBe 3
    r2._2.compare(Double.NaN) shouldBe 0

    val c = obj.presentWithValue(Position("foo"), r2, count).result
    c shouldBe None
  }

  it should "prepare, reduce and present with non-strict, nan and negate with log" in {
    val obj = Entropy[P, S, W](extractor, false, false, true, true, log4 _)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe Option(((1, 1.0/3 * log4(1.0/3))))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe Option(((1, 2.0/3 * log4(2.0/3))))

    val t3 = obj.prepareWithValue(cell3, count)
    t3.get._1 shouldBe 1
    t3.get._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe ((2, (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe ((3, (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))

    val c = obj.presentWithValue(Position("foo"), r2, count).result
    c shouldBe Option(Cell(Position("foo"), getDoubleContent(2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))
  }

  it should "prepare, reduce and present with non-strict, nan and non-negate with log" in {
    val obj = Entropy[P, S, W](extractor, false, false, true, false, log4 _)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe Option(((1, 1.0/3 * log4(1.0/3))))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe Option(((1, 2.0/3 * log4(2.0/3))))

    val t3 = obj.prepareWithValue(cell3, count)
    t3.get._1 shouldBe 1
    t3.get._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe ((2, (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe ((3, (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))

    val c = obj.presentWithValue(Position("foo"), r2, count).result
    c shouldBe Option(Cell(Position("foo"), getDoubleContent(- (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3)))))
  }

  it should "prepare, reduce and present with non-strict, non-nan and negate with log" in {
    val obj = Entropy[P, S, W](extractor, false, false, false, true, log4 _)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe Option(((1, 1.0/3 * log4(1.0/3))))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe Option(((1, 2.0/3 * log4(2.0/3))))

    val t3 = obj.prepareWithValue(cell3, count)
    t3.get._1 shouldBe 1
    t3.get._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe ((2, (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe ((3, (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))

    val c = obj.presentWithValue(Position("foo"), r2, count).result
    c shouldBe Option(Cell(Position("foo"), getDoubleContent(2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))
  }

  it should "prepare, reduce and present with non-strict, non-nan and non-negate with log" in {
    val obj = Entropy[P, S, W](extractor, false, false, false, false, log4 _)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe Option(((1, 1.0/3 * log4(1.0/3))))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe Option(((1, 2.0/3 * log4(2.0/3))))

    val t3 = obj.prepareWithValue(cell3, count)
    t3.get._1 shouldBe 1
    t3.get._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe ((2, (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe ((3, (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))

    val c = obj.presentWithValue(Position("foo"), r2, count).result
    c shouldBe Option(Cell(Position("foo"), getDoubleContent(- (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3)))))
  }

  it should "prepare, reduce and present expanded" in {
    val obj = Entropy[P, S, W](extractor, false)
      .andThenRelocateWithValue((c: Cell[S], e: W) => c.position.append("entropy").toOption)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe Option(((1, 1.0/3 * log2(1.0/3))))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe Option(((1, 2.0/3 * log2(2.0/3))))

    val t3 = obj.prepareWithValue(cell3, count)
    t3.get._1 shouldBe 1
    t3.get._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe ((2, (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))

    val r2 = obj.reduce(r1, t3.get)
    r2._1 shouldBe 3
    r2._2.compare(Double.NaN) shouldBe 0

    val c = obj.presentWithValue(Position("foo"), r2, count).result
    c shouldBe None
  }

  it should "prepare, reduce and present expanded with strict, nan and negate" in {
    val obj = Entropy[P, S, W](extractor, false, true, true, true)
      .andThenRelocateWithValue((c: Cell[S], e: W) => c.position.append("entropy").toOption)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe Option(((1, 1.0/3 * log2(1.0/3))))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe Option(((1, 2.0/3 * log2(2.0/3))))

    val t3 = obj.prepareWithValue(cell3, count)
    t3.get._1 shouldBe 1
    t3.get._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe ((2, (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))

    val r2 = obj.reduce(r1, t3.get)
    r2._1 shouldBe 3
    r2._2.compare(Double.NaN) shouldBe 0

    val c = obj.presentWithValue(Position("foo"), r2, count).result.get
    c.position shouldBe Position("foo", "entropy")
    c.content.value.as[Double].map(_.compare(Double.NaN)) shouldBe Option(0)
  }

  it should "prepare, reduce and present multiple with strict, nan and non-negate" in {
    val obj = Entropy[P, S, W](extractor, false, true, true, false)
      .andThenRelocateWithValue((c: Cell[S], e: W) => c.position.append("entropy").toOption)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe Option(((1, 1.0/3 * log2(1.0/3))))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe Option(((1, 2.0/3 * log2(2.0/3))))

    val t3 = obj.prepareWithValue(cell3, count)
    t3.get._1 shouldBe 1
    t3.get._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe ((2, (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))

    val r2 = obj.reduce(r1, t3.get)
    r2._1 shouldBe 3
    r2._2.compare(Double.NaN) shouldBe 0

    val c = obj.presentWithValue(Position("foo"), r2, count).result.get
    c.position shouldBe Position("foo", "entropy")
    c.content.value.as[Double].map(_.compare(Double.NaN)) shouldBe Option(0)
  }

  it should "prepare, reduce and present expanded with strict, non-nan and negate" in {
    val obj = Entropy[P, S, W](extractor, false, true, false, true)
      .andThenRelocateWithValue((c: Cell[S], e: W) => c.position.append("entropy").toOption)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe Option(((1, 1.0/3 * log2(1.0/3))))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe Option(((1, 2.0/3 * log2(2.0/3))))

    val t3 = obj.prepareWithValue(cell3, count)
    t3.get._1 shouldBe 1
    t3.get._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe ((2, (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))

    val r2 = obj.reduce(r1, t3.get)
    r2._1 shouldBe 3
    r2._2.compare(Double.NaN) shouldBe 0

    val c = obj.presentWithValue(Position("foo"), r2, count).result
    c shouldBe None
  }

  it should "prepare, reduce and present expanded with strict, non-nan and non-negate" in {
    val obj = Entropy[P, S, W](extractor, false, true, false, false)
      .andThenRelocateWithValue((c: Cell[S], e: W) => c.position.append("entropy").toOption)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe Option(((1, 1.0/3 * log2(1.0/3))))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe Option(((1, 2.0/3 * log2(2.0/3))))

    val t3 = obj.prepareWithValue(cell3, count)
    t3.get._1 shouldBe 1
    t3.get._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe ((2, (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))

    val r2 = obj.reduce(r1, t3.get)
    r2._1 shouldBe 3
    r2._2.compare(Double.NaN) shouldBe 0

    val c = obj.presentWithValue(Position("foo"), r2, count).result
    c shouldBe None
  }

  it should "prepare, reduce and present expanded with non-strict, nan and negate" in {
    val obj = Entropy[P, S, W](extractor, false, false, true, true)
      .andThenRelocateWithValue((c: Cell[S], e: W) => c.position.append("entropy").toOption)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe Option(((1, 1.0/3 * log2(1.0/3))))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe Option(((1, 2.0/3 * log2(2.0/3))))

    val t3 = obj.prepareWithValue(cell3, count)
    t3.get._1 shouldBe 1
    t3.get._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe ((2, (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe ((3, (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))

    val c = obj.presentWithValue(Position("foo"), r2, count).result
    c shouldBe Option(Cell(Position("foo", "entropy"), getDoubleContent(2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))
  }

  it should "prepare, reduce and present expanded with non-strict, nan and non-negate" in {
    val obj = Entropy[P, S, W](extractor, false, false, true, false)
      .andThenRelocateWithValue((c: Cell[S], e: W) => c.position.append("entropy").toOption)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe Option(((1, 1.0/3 * log2(1.0/3))))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe Option(((1, 2.0/3 * log2(2.0/3))))

    val t3 = obj.prepareWithValue(cell3, count)
    t3.get._1 shouldBe 1
    t3.get._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe ((2, (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe ((3, (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))

    val c = obj.presentWithValue(Position("foo"), r2, count).result
    c shouldBe Option(Cell(Position("foo", "entropy"), getDoubleContent(- (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3)))))
  }

  it should "prepare, reduce and present expanded with non-strict, non-nan and negate" in {
    val obj = Entropy[P, S, W](extractor, false, false, false, true)
      .andThenRelocateWithValue((c: Cell[S], e: W) => c.position.append("entropy").toOption)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe Option(((1, 1.0/3 * log2(1.0/3))))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe Option(((1, 2.0/3 * log2(2.0/3))))

    val t3 = obj.prepareWithValue(cell3, count)
    t3.get._1 shouldBe 1
    t3.get._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe ((2, (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe ((3, (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))

    val c = obj.presentWithValue(Position("foo"), r2, count).result
    c shouldBe Option(Cell(Position("foo", "entropy"), getDoubleContent(2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))
  }

  it should "prepare, reduce and present expanded with non-strict, non-nan and non-negate" in {
    val obj = Entropy[P, S, W](extractor, false, false, false, false)
      .andThenRelocateWithValue((c: Cell[S], e: W) => c.position.append("entropy").toOption)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe Option(((1, 1.0/3 * log2(1.0/3))))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe Option(((1, 2.0/3 * log2(2.0/3))))

    val t3 = obj.prepareWithValue(cell3, count)
    t3.get._1 shouldBe 1
    t3.get._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe ((2, (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe ((3, (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))

    val c = obj.presentWithValue(Position("foo"), r2, count).result
    c shouldBe Option(Cell(Position("foo", "entropy"), getDoubleContent(- (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3)))))
  }

  it should "prepare, reduce and present expanded with log" in {
    val obj = Entropy[P, S, W](extractor, false, log = log4 _)
      .andThenRelocateWithValue((c: Cell[S], e: W) => c.position.append("entropy").toOption)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe Option(((1, 1.0/3 * log4(1.0/3))))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe Option(((1, 2.0/3 * log4(2.0/3))))

    val t3 = obj.prepareWithValue(cell3, count)
    t3.get._1 shouldBe 1
    t3.get._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe ((2, (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))

    val r2 = obj.reduce(r1, t3.get)
    r2._1 shouldBe 3
    r2._2.compare(Double.NaN) shouldBe 0

    val c = obj.presentWithValue(Position("foo"), r2, count).result
    c shouldBe None
  }

  it should "prepare, reduce and present expanded with strict, nan and negate with log" in {
    val obj = Entropy[P, S, W](extractor, false, true, true, true, log4 _)
      .andThenRelocateWithValue((c: Cell[S], e: W) => c.position.append("entropy").toOption)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe Option(((1, 1.0/3 * log4(1.0/3))))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe Option(((1, 2.0/3 * log4(2.0/3))))

    val t3 = obj.prepareWithValue(cell3, count)
    t3.get._1 shouldBe 1
    t3.get._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe ((2, (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))

    val r2 = obj.reduce(r1, t3.get)
    r2._1 shouldBe 3
    r2._2.compare(Double.NaN) shouldBe 0

    val c = obj.presentWithValue(Position("foo"), r2, count).result.get
    c.position shouldBe Position("foo", "entropy")
    c.content.value.as[Double].map(_.compare(Double.NaN)) shouldBe Option(0)
  }

  it should "prepare, reduce and present expanded with strict, nan and non-negate with log" in {
    val obj = Entropy[P, S, W](extractor, false, true, true, false, log4 _)
      .andThenRelocateWithValue((c: Cell[S], e: W) => c.position.append("entropy").toOption)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe Option(((1, 1.0/3 * log4(1.0/3))))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe Option(((1, 2.0/3 * log4(2.0/3))))

    val t3 = obj.prepareWithValue(cell3, count)
    t3.get._1 shouldBe 1
    t3.get._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe ((2, (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))

    val r2 = obj.reduce(r1, t3.get)
    r2._1 shouldBe 3
    r2._2.compare(Double.NaN) shouldBe 0

    val c = obj.presentWithValue(Position("foo"), r2, count).result.get
    c.position shouldBe Position("foo", "entropy")
    c.content.value.as[Double].map(_.compare(Double.NaN)) shouldBe Option(0)
  }

  it should "prepare, reduce and present expanded with strict, non-nan and negate with log" in {
    val obj = Entropy[P, S, W](extractor, false, true, false, true, log4 _)
      .andThenRelocateWithValue((c: Cell[S], e: W) => c.position.append("entropy").toOption)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe Option(((1, 1.0/3 * log4(1.0/3))))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe Option(((1, 2.0/3 * log4(2.0/3))))

    val t3 = obj.prepareWithValue(cell3, count)
    t3.get._1 shouldBe 1
    t3.get._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe ((2, (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))

    val r2 = obj.reduce(r1, t3.get)
    r2._1 shouldBe 3
    r2._2.compare(Double.NaN) shouldBe 0

    val c = obj.presentWithValue(Position("foo"), r2, count).result
    c shouldBe None
  }

  it should "prepare, reduce and present expanded with strict, non-nan and non-negate with log" in {
    val obj = Entropy[P, S, W](extractor, false, true, false, false, log4 _)
      .andThenRelocateWithValue((c: Cell[S], e: W) => c.position.append("entropy").toOption)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe Option(((1, 1.0/3 * log4(1.0/3))))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe Option(((1, 2.0/3 * log4(2.0/3))))

    val t3 = obj.prepareWithValue(cell3, count)
    t3.get._1 shouldBe 1
    t3.get._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe ((2, (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))

    val r2 = obj.reduce(r1, t3.get)
    r2._1 shouldBe 3
    r2._2.compare(Double.NaN) shouldBe 0

    val c = obj.presentWithValue(Position("foo"), r2, count).result
    c shouldBe None
  }

  it should "prepare, reduce and present expanded with non-strict, nan and negate with log" in {
    val obj = Entropy[P, S, W](extractor, false, false, true, true, log4 _)
      .andThenRelocateWithValue((c: Cell[S], e: W) => c.position.append("entropy").toOption)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe Option(((1, 1.0/3 * log4(1.0/3))))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe Option(((1, 2.0/3 * log4(2.0/3))))

    val t3 = obj.prepareWithValue(cell3, count)
    t3.get._1 shouldBe 1
    t3.get._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe ((2, (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe ((3, (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))

    val c = obj.presentWithValue(Position("foo"), r2, count).result
    c shouldBe Option(Cell(Position("foo", "entropy"), getDoubleContent(2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))
  }

  it should "prepare, reduce and present expanded with non-strict, nan and non-negate with log" in {
    val obj = Entropy[P, S, W](extractor, false, false, true, false, log4 _)
      .andThenRelocateWithValue((c: Cell[S], e: W) => c.position.append("entropy").toOption)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe Option(((1, 1.0/3 * log4(1.0/3))))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe Option(((1, 2.0/3 * log4(2.0/3))))

    val t3 = obj.prepareWithValue(cell3, count)
    t3.get._1 shouldBe 1
    t3.get._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe ((2, (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe ((3, (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))

    val c = obj.presentWithValue(Position("foo"), r2, count).result
    c shouldBe Option(Cell(Position("foo", "entropy"), getDoubleContent(- (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3)))))
  }

  it should "prepare, reduce and present expanded with non-strict, non-nan and negate with log" in {
    val obj = Entropy[P, S, W](extractor, false, false, false, true, log4 _)
      .andThenRelocateWithValue((c: Cell[S], e: W) => c.position.append("entropy").toOption)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe Option(((1, 1.0/3 * log4(1.0/3))))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe Option(((1, 2.0/3 * log4(2.0/3))))

    val t3 = obj.prepareWithValue(cell3, count)
    t3.get._1 shouldBe 1
    t3.get._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe ((2, (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe ((3, (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))

    val c = obj.presentWithValue(Position("foo"), r2, count).result
    c shouldBe Option(Cell(Position("foo", "entropy"), getDoubleContent(2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))
  }

  it should "prepare, reduce and present expanded with non-strict, non-nan and non-negate with log" in {
    val obj = Entropy[P, S, W](extractor, false, false, false, false, log4 _)
      .andThenRelocateWithValue((c: Cell[S], e: W) => c.position.append("entropy").toOption)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe Option(((1, 1.0/3 * log4(1.0/3))))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe Option(((1, 2.0/3 * log4(2.0/3))))

    val t3 = obj.prepareWithValue(cell3, count)
    t3.get._1 shouldBe 1
    t3.get._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe ((2, (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe ((3, (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))

    val c = obj.presentWithValue(Position("foo"), r2, count).result
    c shouldBe Option(Cell(Position("foo", "entropy"), getDoubleContent(- (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3)))))
  }

  it should "filter" in {
    Entropy[P, S, W](extractor, true).prepareWithValue(cell1, count) shouldBe Option(((1, 1.0/3 * log2(1.0/3))))
    Entropy[P, S, W](extractor, false).prepareWithValue(cell1, count) shouldBe Option(((1, 1.0/3 * log2(1.0/3))))
    Entropy[P, S, W](extractor, true).prepareWithValue(cell3, count) shouldBe None
    Entropy[P, S, W](extractor, false)
      .prepareWithValue(cell3, count)
      .map { case (l, d) => (l, d.compare(Double.NaN)) } shouldBe Option((1, 0))
  }
}

class TestWithPrepareAggregator extends TestAggregators {
  type X = Value[String] :: HNil

  val str = Cell(Position("x"), getStringContent("foo"))
  val dbl = Cell(Position("y"), getDoubleContent(3.14))
  val lng = Cell(Position("z"), getLongContent(42))

  val ext = Map(
    Position("x") -> getDoubleContent(1),
    Position("y") -> getDoubleContent(2),
    Position("z") -> getDoubleContent(3)
  )

  def prepare(cell: Cell[X]): Content = cell.content.value match {
    case LongValue(_) => cell.content
    case DoubleValue(_) => getStringContent("not.supported")
    case StringValue(s) => getLongContent(s.length)
  }

  def prepareWithValue(cell: Cell[X], ext: Map[Position[X], Content]): Content =
    (cell.content.value, ext(cell.position).value) match {
      case (LongValue(l), DoubleValue(d)) => getLongContent(l * d.toLong)
      case (DoubleValue(_), _) => getStringContent("not.supported")
      case (StringValue(s), _) => getLongContent(s.length)
    }

  "An Aggregator" should "withPrepare prepare correctly" in {
    val obj = Maximum[X, X](false).withPrepare(prepare)

    obj.prepare(str) shouldBe Option(3.0)
    obj.prepare(dbl).map(_.compare(Double.NaN)) shouldBe Option(0)
    obj.prepare(lng) shouldBe Option(42.0)
  }

  it should "withPrepareWithValue correctly (without value)" in {
    val obj = WeightedSums[X, X, Map[Position[X], Content]](
      ExtractWithPosition().andThenPresent(_.value.as[Double]),
      false
    ).withPrepare(prepare)

    obj.prepareWithValue(str, ext) shouldBe Option(3.0)
    obj.prepareWithValue(dbl, ext).map(_.compare(Double.NaN)) shouldBe Option(0)
    obj.prepareWithValue(lng, ext) shouldBe Option(3 * 42.0)
  }

  it should "withPrepareWithVaue correctly" in {
    val obj = WeightedSums[X, X, Map[Position[X], Content]](
      ExtractWithPosition().andThenPresent(_.value.as[Double]),
      false
    ).withPrepareWithValue(prepareWithValue)

    obj.prepareWithValue(str, ext) shouldBe Option(3.0)
    obj.prepareWithValue(dbl, ext).map(_.compare(Double.NaN)) shouldBe Option(0)
    obj.prepareWithValue(lng, ext) shouldBe Option(3 * 3 * 42.0)
  }
}

class TestAndThenMutateAggregator extends TestAggregators {
  type X = Value[String] :: HNil

  val x = Position("x")
  val y = Position("y")
  val z = Position("z")

  val ext = Map(x -> getDoubleContent(3), y -> getDoubleContent(2), z -> getDoubleContent(1))

  def mutate(cell: Cell[X]): Option[Content] = cell.position(_0) match {
    case StringValue("x") => cell.content.toOption
    case StringValue("y") => getStringContent("not.supported").toOption
    case StringValue("z") => getLongContent(123).toOption
  }

  def mutateWithValue(cell: Cell[X], ext: Map[Position[X], Content]): Option[Content] =
    (cell.position(_0), ext(cell.position).value) match {
      case (StringValue("x"), DoubleValue(d)) => getStringContent("x" * d.toInt).toOption
      case (StringValue("y"), _) => getStringContent("not.supported").toOption
      case (StringValue("z"), DoubleValue(d)) => getLongContent(d.toLong).toOption
    }

  "An Aggregator" should "andThenMutate correctly" in {
    val obj = Maximum[X, X]().andThenMutate(mutate)

    obj.present(x, -1).result shouldBe Option(Cell(x, getDoubleContent(-1)))
    obj.present(y, 3.14).result shouldBe Option(Cell(y, getStringContent("not.supported")))
    obj.present(z, 42).result shouldBe Option(Cell(z, getLongContent(123)))
  }

  it should "andThenMutateWithValue correctly (without value)" in {
    val obj = WeightedSums[X, X, Map[Position[X], Content]](ExtractWithPosition().andThenPresent(_.value.as[Double]))
      .andThenMutate(mutate)

    obj.presentWithValue(x, -1, ext).result shouldBe Option(Cell(x, getDoubleContent(-1)))
    obj.presentWithValue(y, 3.14, ext).result shouldBe Option(Cell(y, getStringContent("not.supported")))
    obj.presentWithValue(z, 42, ext).result shouldBe Option(Cell(z, getLongContent(123)))
  }

  it should "andThenMutateWithValue correctly" in {
    val obj = WeightedSums[X, X, Map[Position[X], Content]](ExtractWithPosition().andThenPresent(_.value.as[Double]))
      .andThenMutateWithValue(mutateWithValue)

    obj.presentWithValue(x, -1, ext).result shouldBe Option(Cell(x, getStringContent("xxx")))
    obj.presentWithValue(y, 3.14, ext).result shouldBe Option(Cell(y, getStringContent("not.supported")))
    obj.presentWithValue(z, 42, ext).result shouldBe Option(Cell(z, getLongContent(1)))
  }
}

class TestCountMapHistogram extends TestAggregators {
  type Q = Value[String] :: Value[String] :: HNil

  val cell1 = Cell(Position("foo", "one"), getStringContent("bar"))
  val cell2 = Cell(Position("foo", "two"), getDoubleContent(2))
  val cell3 = Cell(Position("foo", "three"), getStringContent("baz"))
  val cell4 = Cell(Position("foo", "four"), getStringContent("bar"))

  val name = Locate.AppendContentString[S]

  "A CountMapHistogram" should "prepare, reduce and present" in {
    val obj = CountMapHistogram[P, S, Q](name)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(Map(getStringContent("bar") -> 1L))

    val t2 = obj.prepare(cell2)
    t2 shouldBe None

    val t3 = obj.prepare(cell3)
    t3 shouldBe Option(Map(getStringContent("baz") -> 1L))

    val t4 = obj.prepare(cell4)
    t4 shouldBe Option(Map(getStringContent("bar") -> 1L))

    val r1 = obj.reduce(t1.get, t3.get)
    r1 shouldBe Map(getStringContent("bar") -> 1L, getStringContent("baz") -> 1L)

    val r2 = obj.reduce(r1, t4.get)
    r2 shouldBe Map(getStringContent("bar") -> 2L, getStringContent("baz") -> 1L)

    val c = obj.present(Position("foo"), r2).result.toList
    c.sortBy(_.position) shouldBe List(
      Cell(Position("foo", "bar"), getLongContent(2)),
      Cell(Position("foo", "baz"), getLongContent(1))
    )
  }

  it should "prepare, reduce and present without filter" in {
    val obj = CountMapHistogram[P, S, Q](name, false)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(Map(getStringContent("bar") -> 1L))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(Map(getDoubleContent(2) -> 1L))

    val t3 = obj.prepare(cell3)
    t3 shouldBe Option(Map(getStringContent("baz") -> 1L))

    val t4 = obj.prepare(cell4)
    t4 shouldBe Option(Map(getStringContent("bar") -> 1L))

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe Map(getStringContent("bar") -> 1L, getDoubleContent(2) -> 1L)

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe Map(getDoubleContent(2) -> 1L, getStringContent("bar") -> 1L, getStringContent("baz") -> 1L)

    val r3 = obj.reduce(r2, t4.get)
    r3 shouldBe Map(getDoubleContent(2) -> 1L, getStringContent("bar") -> 2L, getStringContent("baz") -> 1L)

    val c = obj.present(Position("foo"), r3).result.toList
    c.sortBy(_.position) shouldBe List(
      Cell(Position("foo", "2.0"), getLongContent(1)),
      Cell(Position("foo", "bar"), getLongContent(2)),
      Cell(Position("foo", "baz"), getLongContent(1))
    )
  }

  it should "prepare, reduce and present expanded" in {
    val obj = CountMapHistogram[P, S, Q](name).andThenRelocate(_.position.append("hist").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(Map(getStringContent("bar") -> 1L))

    val t2 = obj.prepare(cell2)
    t2 shouldBe None

    val t3 = obj.prepare(cell3)
    t3 shouldBe Option(Map(getStringContent("baz") -> 1L))

    val t4 = obj.prepare(cell4)
    t4 shouldBe Option(Map(getStringContent("bar") -> 1L))

    val r1 = obj.reduce(t1.get, t3.get)
    r1 shouldBe Map(getStringContent("bar") -> 1L, getStringContent("baz") -> 1L)

    val r2 = obj.reduce(r1, t4.get)
    r2 shouldBe Map(getStringContent("bar") -> 2L, getStringContent("baz") -> 1L)

    val c = obj.present(Position("foo"), r2).result.toList
    c.sortBy(_.position) shouldBe List(
      Cell(Position("foo", "bar", "hist"), getLongContent(2)),
      Cell(Position("foo", "baz", "hist"), getLongContent(1))
    )
  }

  it should "prepare, reduce and present expanded without filter" in {
    val obj = CountMapHistogram[P, S, Q](name, false).andThenRelocate(_.position.append("hist").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(Map(getStringContent("bar") -> 1L))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(Map(getDoubleContent(2) -> 1L))

    val t3 = obj.prepare(cell3)
    t3 shouldBe Option(Map(getStringContent("baz") -> 1L))

    val t4 = obj.prepare(cell4)
    t4 shouldBe Option(Map(getStringContent("bar") -> 1L))

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe Map(getStringContent("bar") -> 1L, getDoubleContent(2) -> 1L)

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe Map(getDoubleContent(2) -> 1L, getStringContent("bar") -> 1L, getStringContent("baz") -> 1L)

    val r3 = obj.reduce(r2, t4.get)
    r3 shouldBe Map(getDoubleContent(2) -> 1L, getStringContent("bar") -> 2L, getStringContent("baz") -> 1L)

    val c = obj.present(Position("foo"), r3).result.toList
    c.sortBy(_.position) shouldBe List(
      Cell(Position("foo", "2.0", "hist"), getLongContent(1)),
      Cell(Position("foo", "bar", "hist"), getLongContent(2)),
      Cell(Position("foo", "baz", "hist"), getLongContent(1))
    )
  }
}

class TestConfusionMatrixAggregator extends TestAggregators {
  import commbank.grimlock.framework.environment.implicits.stringToValue

  type Q = Value[Boolean] :: Value[Double] :: P

  val cell1 = binaryCell(Position("foo", "bar"), 0.5, true)
  val cell2 = binaryCell(Position("foo", "baz"), 0.5, true)
  val map = makeThresholdMap(_3, _0, _1, Map("bar" -> 0.3, "baz" -> 0.7))
  val pos = Position("foo")
  val posPrepend = Position("baz", "foo")

  val accuracy = appendString[S]("accuracy")
  val f1score = appendString[S]("f1score")
  val fdr = appendString[S]("fdr")
  val fn = appendString[S]("fn")
  val fp = appendString[S]("fp")
  val precision = appendString[S]("precision")
  val recall = appendString[S]("recall")
  val tn = appendString[S]("tn")
  val tp = appendString[S]("tp")

  val accuracyP = appendString[P]("accuracy")
  val f1scoreP = appendString[P]("f1score")
  val fdrP = appendString[P]("fdr")
  val fnP = appendString[P]("fn")
  val fpP = appendString[P]("fp")
  val precisionP = appendString[P]("precision")
  val recallP = appendString[P]("recall")
  val tnP = appendString[P]("tn")
  val tpP = appendString[P]("tp")

  def binaryCell[
    Z <: HList,
    W <: HList
  ](
    pos: Position[Z],
    score: Double,
    outcome: Boolean
  )(implicit
    ev1: Position.PrependConstraints.Aux[Z, Value[Double], Value[Double] :: Z],
    ev2: Position.PrependConstraints.Aux[Value[Double] :: Z, Value[Boolean], W]
  ) = Cell(pos.prepend(score).prepend(outcome), getBooleanContent(true))

  def makeThresholdMap[
    D <: Nat,
    Ou <: Nat,
    Sc <: Nat,
    Id
  ](
    dim: D,
    outcome: Ou,
    score: Sc,
    map: Map[Id, Double]
  )(implicit
    ev1: Position.IndexConstraints.Aux[Q, D, Value[Id]],
    ev2: Position.IndexConstraints.Aux[Q, Ou, Value[Boolean]],
    ev3: Position.IndexConstraints.Aux[Q, Sc, Value[Double]]
  ): Cell[Q] => Option[(Boolean, Boolean)] = {
    cell: Cell[Q] => for {
      threshold <- map.get(cell.position(dim).value)
      o <- cell.position(outcome).as[Boolean]
      s <- cell.position(score).as[Double].map(_ > threshold)
    } yield (o, s)
  }

  "A ConfusionMatrix" should "prepare, reduce and present" in {
    val obj = ConfusionMatrixAggregator(
      map,
      accuracy,
      f1score,
      fdr,
      fn,
      fp,
      precision,
      recall,
      tn,
      tp
    )

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(ConfusionMatrix(tp = 1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(ConfusionMatrix(fn = 1))

    val r = obj.reduce(t1.get, t2.get)
    r shouldBe ConfusionMatrix(tp = 1, fn = 1)

    val c = obj.present(pos, r).result
    c shouldBe List(
      Cell(accuracy(pos).get, getDoubleContent(0.5)),
      Cell(f1score(pos).get, getDoubleContent(0.6666666666666666)),
      Cell(fdr(pos).get, getDoubleContent(0.0)),
      Cell(fn(pos).get, getDoubleContent(1.0)),
      Cell(fp(pos).get, getDoubleContent(0.0)),
      Cell(precision(pos).get, getDoubleContent(1.0)),
      Cell(recall(pos).get, getDoubleContent(0.5)),
      Cell(tn(pos).get, getDoubleContent(0.0)),
      Cell(tp(pos).get, getDoubleContent(1.0))
    )
  }

  it should "prepare, reduce and present expanded" in {
    val obj = ConfusionMatrixAggregator(
      map,
      accuracy,
      f1score,
      fdr,
      fn,
      fp,
      precision,
      recall,
      tn,
      tp
    ).andThenRelocate(_.position.prepend("baz").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(ConfusionMatrix(tp = 1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(ConfusionMatrix(fn = 1))

    val r = obj.reduce(t1.get, t2.get)
    r shouldBe ConfusionMatrix(tp = 1, fn = 1)

    val c = obj.present(pos, r).result.toList
    c.sortBy(_.position) shouldBe List(
      Cell(accuracyP(posPrepend).get, getDoubleContent(0.5)),
      Cell(f1scoreP(posPrepend).get, getDoubleContent(0.6666666666666666)),
      Cell(fdrP(posPrepend).get, getDoubleContent(0.0)),
      Cell(fnP(posPrepend).get, getDoubleContent(1.0)),
      Cell(fpP(posPrepend).get, getDoubleContent(0.0)),
      Cell(precisionP(posPrepend).get, getDoubleContent(1.0)),
      Cell(recallP(posPrepend).get, getDoubleContent(0.5)),
      Cell(tnP(posPrepend).get, getDoubleContent(0.0)),
      Cell(tpP(posPrepend).get, getDoubleContent(1.0))
    )
  }
}

