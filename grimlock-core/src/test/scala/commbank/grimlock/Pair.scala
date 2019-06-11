// Copyright 2019 Commonwealth Bank of Australia
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

import commbank.grimlock.framework.Cell
import commbank.grimlock.framework.content.Content
import commbank.grimlock.framework.encoding.{
  DateCodec,
  DecimalCodec,
  DoubleCodec,
  DoubleValue,
  IntCodec,
  StringCodec,
  StringValue,
  TimestampCodec
}
import commbank.grimlock.framework.metadata.Type
import commbank.grimlock.framework.pair.{ GeneratePair, PairCodec, PairSchema, PairSpec, PairValue }
import commbank.grimlock.framework.position.Position

import java.sql.Timestamp
import java.util.Date

import scala.math.BigDecimal
import scala.reflect.runtime.universe.TypeTag

import shapeless.nat._0

class TestPairCodec extends TestGrimlock {
  val pair1 = (1.0, "abcde")
  val pair2 = ("foo", 7)
  val pair3 = ("baz", 15)
  val pair4 = ("baz", 2)

  "A PairCodec" should "have a name" in {
    PairCodec(DoubleCodec, StringCodec).toShortString shouldBe "pair,double,string,(,)"
  }

  it should "decode a correct value" in {
    PairCodec(DoubleCodec, StringCodec, ')', '#', '(').decode(")1.0#abcde(") shouldBe Option(pair1)
  }

  it should "encode a correct value" in {
    PairCodec(StringCodec, IntCodec, '{', '_', '}').encode(pair2) shouldBe "{foo_7}"
  }

  it should "compare a correct value" in {
    PairCodec(StringCodec, IntCodec).compare(pair2, pair3) > 0 shouldBe true
    PairCodec(StringCodec, IntCodec).compare(pair4, pair3) < 0 shouldBe true
    PairCodec(StringCodec, IntCodec).compare(pair4, pair4) shouldBe 0
  }

  it should "box correctly" in {
    PairCodec(DoubleCodec, StringCodec, '*', '*', '*').box(pair1) shouldBe
      PairValue(pair1, PairCodec(DoubleCodec, StringCodec, '*', '*', '*'))
  }

  it should "return fields" in {
    PairCodec(IntCodec, IntCodec).converters.size shouldBe 0

    PairCodec(StringCodec, DoubleCodec).date.isEmpty shouldBe true
    PairCodec(StringCodec, DoubleCodec).integral.isEmpty shouldBe true
    PairCodec(StringCodec, DoubleCodec).numeric.isEmpty shouldBe true
  }
}

class TestPairValue extends TestGrimlock {
  import commbank.grimlock.framework.environment.implicits.{ booleanToValue, doubleToValue, intToValue }

  val cdcFoo = PairCodec(DoubleCodec, StringCodec, '{', '*', '}')
  val cdcBar = PairCodec(StringCodec, DoubleCodec)
  val foo = (1.0, "abc")
  val fooBig = (2.0, "abc")
  val fooBig2 = (1.0, "def")
  val bar = ("def", 1.001)
  val dvfoo = PairValue(foo, cdcFoo)
  val dvfooBig = PairValue(fooBig, cdcFoo)
  val dvfooBig2 = PairValue(fooBig2, cdcFoo)
  val dvbar = PairValue(bar, cdcBar)

  "A PairValue" should "return its short string" in {
    dvfoo.toShortString shouldBe "{1.0*abc}"
  }

  it should "not return a date" in {
    dvfoo.as[Date] shouldBe None
  }

  it should "not return a string" in {
    dvfoo.as[String] shouldBe None
    dvfoo.as[String] shouldBe None
  }

  it should "not return a double" in {
    dvfoo.as[Double] shouldBe None
  }

  it should "not return a float" in {
    dvfoo.as[Float] shouldBe None
  }

  it should "not return a long" in {
    dvfoo.as[Long] shouldBe None
  }

  it should "not return a int" in {
    dvfoo.as[Int] shouldBe None
  }

  it should "not return a boolean" in {
    dvfoo.as[Boolean] shouldBe None
  }

  it should "not return a type" in {
    dvfoo.as[Type] shouldBe None
  }

  it should "not return a timestamp" in {
    dvfoo.as[Timestamp] shouldBe None
  }

  it should "not return a byte array" in {
    dvfoo.as[Array[Byte]] shouldBe None
  }

  it should "not return a decimal" in {
    dvfoo.as[BigDecimal] shouldBe None
  }

  it should "return a pair when given correct types" in {
    dvfoo.as[(Double, String)] shouldBe Option((1.0, "abc"))
    dvbar.as[(String, Double)] shouldBe Option(("def", 1.001))
  }

  it should "not return a pair when given incorrect types" in {
    dvfoo.as[(Timestamp, Date)] shouldBe None
    dvbar.as[(BigDecimal, Boolean)] shouldBe None
  }

  it should "equal itself" in {
    dvfoo.equ(dvfoo) shouldBe true
    dvfoo.equ(PairValue(foo, cdcFoo)) shouldBe true
  }

  it should "not equal another pair" in {
    StringValue("foo").equ(DoubleValue(1.0))
    dvfoo.equ(dvbar) shouldBe false
    dvfoo.equ(dvfooBig) shouldBe false
  }

  it should "not equal another value" in {
    dvfoo.equ(2) shouldBe false
    dvfoo.equ(2.0) shouldBe false
    dvfoo.equ(false) shouldBe false
  }

  it should "match a matching pattern" in {
    dvfoo.like("^\\{...\\*...\\}".r) shouldBe true
  }

  it should "not match a non-existing pattern" in {
    dvfoo.like("^_".r) shouldBe false
  }

  it should "identify a smaller value calling lss" in {
    dvfoo.lss(dvfooBig) shouldBe true
    dvfoo.lss(dvfooBig2) shouldBe true
  }

  it should "not identify an equal value calling lss" in {
    dvbar.lss(dvbar) shouldBe false
  }

  it should "not identify a greater value calling lss" in {
    dvfooBig.lss(dvfoo) shouldBe false
  }

  it should "not identify another value calling lss" in {
    dvfooBig.lss(dvfoo) shouldBe false
    dvfooBig2.lss(dvfoo) shouldBe false
  }

  it should "identify a smaller value calling leq" in {
    dvfoo.leq(dvfooBig) shouldBe true
    dvfoo.leq(dvfooBig2) shouldBe true
  }

  it should "identify an equal value calling leq" in {
    dvbar.leq(dvbar) shouldBe true
  }

  it should "not identify a greater value calling leq" in {
    dvfooBig.leq(dvfoo) shouldBe false
    dvfooBig2.leq(dvfoo) shouldBe false
  }

  it should "not identify another value calling leq" in {
    dvbar.leq(2) shouldBe false
  }

  it should "not identify a smaller value calling gtr" in {
    dvfoo.gtr(dvfooBig) shouldBe false
    dvfoo.gtr(dvfooBig2) shouldBe false
  }

  it should "not identify an equal value calling gtr" in {
    dvbar.gtr(dvbar) shouldBe false
  }

  it should "identify a greater value calling gtr" in {
    dvfooBig.gtr(dvfoo) shouldBe true
    dvfooBig2.gtr(dvfoo) shouldBe true
  }

  it should "not identify another value calling gtr" in {
    dvbar.gtr(2) shouldBe false
  }

  it should "not identify a smaller value calling geq" in {
    dvfoo.geq(dvfooBig) shouldBe false
    dvfoo.geq(dvfooBig2) shouldBe false
  }

  it should "identify an equal value calling geq" in {
    dvbar.geq(dvbar) shouldBe true
  }

  it should "identify a greater value calling geq" in {
    dvfooBig.geq(dvfoo) shouldBe true
    dvfooBig2.geq(dvfoo) shouldBe true
  }

  it should "not identify another value calling geq" in {
    dvbar.geq(2) shouldBe false
  }
}

class TestPairSchema extends TestGrimlock {
  val dfmt = new java.text.SimpleDateFormat("dd/MM/yyyy")
  val date2001 = dfmt.parse("01/01/2001")
  val one = BigDecimal(1.0)

  "A PairSchema" should "return its string representation" in {
    PairSchema[String, Double]().toShortString(PairCodec(StringCodec, DoubleCodec)) shouldBe "pair"
    PairSchema[Date, Timestamp]().toShortString(PairCodec(DateCodec(), TimestampCodec)) shouldBe "pair"
  }

  it should "validate a correct value" in {
    PairSchema[String, Double]().validate(PairValue(("abc", 1.0), PairCodec(StringCodec, DoubleCodec))) shouldBe true
    PairSchema[Timestamp, BigDecimal]()
      .validate(PairValue((new Timestamp(date2001.getTime), one), PairCodec(TimestampCodec, DecimalCodec(5, 4)))) shouldBe true
  }

  it should "not validate an incorrect value" in {
    // TODO: it allows all values of type (X, Y). So this test is redundant
  }

  it should "parse correctly" in {
    PairSchema.fromShortString("pair", PairCodec(IntCodec, StringCodec)) shouldBe Option(PairSchema[Int, String])
  }

  it should "not parse an incorrect string" in {
    PairSchema.fromShortString("Xair", PairCodec(StringCodec, IntCodec)) shouldBe None
  }
}

class TestGeneratePair extends TestAggregators {
  import commbank.grimlock.framework.environment.implicits.stringToValue

  def getPairContent[
    X : TypeTag,
    Y : TypeTag
  ](
    left: X,
    right: Y,
    codec: PairCodec[X, Y],
    schema: PairSchema[X ,Y] = PairSchema[X, Y]()
  ): Content =
    Content(schema, PairValue((left, right), codec))

  val cellD1 = Cell(Position("left", "foo"), getDoubleContent(1))
  val cellD2 = Cell(Position("right", "foo"), getDoubleContent(2))
  val tD1 = List(("left", Left(1)))
  val tD2 = List(("right", Right(2)))

  val cellS1 = Cell(Position("left", "bar"), getStringContent("foo"))
  val cellS2 = Cell(Position("right", "bar"), getStringContent("baz"))
  val tS1 = List(("left", Left("foo")))
  val tS2 = List(("right", Right("baz")))

  val stringDefault = "string.default"
  val doubleDefault: Double = -999.0

  "A GeneratePair" should "prepare, reduce and present Doubles" in {
    val obj = GeneratePair[P, S, _0, Double, Double](
      PairSpec("left", DoubleCodec),
      PairSpec("right", DoubleCodec),
      _0
    )

    val t1 = obj.prepare(cellD1)
    t1 shouldBe Option(tD1)

    val t2 = obj.prepare(cellD2)
    t2 shouldBe Option(tD2)

    val r = obj.reduce(t1.get, t2.get)
    r shouldBe (tD1 ++ tD2)

    val c = obj.present(Position("foo"), r).result
    c shouldBe Option(Cell(Position("foo"), getPairContent(1.0, 2.0, PairCodec(DoubleCodec, DoubleCodec))))
  }

  it should "prepare, reduce and present Strings" in {
    val obj = GeneratePair[P, S, _0, String, String](
      PairSpec("left", StringCodec),
      PairSpec("right", StringCodec),
      _0
    )

    val t1 = obj.prepare(cellS1)
    t1 shouldBe Option(tS1)

    val t2 = obj.prepare(cellS2)
    t2 shouldBe Option(tS2)

    val r = obj.reduce(t1.get, t2.get)
    r shouldBe (tS1 ++ tS2)

    val c = obj.present(Position("foo"), r).result
    c shouldBe Option(Cell(Position("foo"), getPairContent("foo", "baz", PairCodec(StringCodec, StringCodec))))
  }

  it should "prepare, reduce and present expanded" in {
    val obj = GeneratePair[P, S, _0, Double, Double](
      PairSpec("left", DoubleCodec),
      PairSpec("right", DoubleCodec),
      _0
    ).andThenRelocate(_.position.append("pair").toOption)

    val t1 = obj.prepare(cellD1)
    val t2 = obj.prepare(cellD2)
    val r = obj.reduce(t1.get, t2.get)
    val c = obj.present(Position("foo"), r).result
    c shouldBe Option(Cell(Position("foo", "pair"), getPairContent(1.0, 2.0, PairCodec(DoubleCodec, DoubleCodec))))
  }

  it should "present with default values for left" in {
    val obj = GeneratePair[P, S, _0, String, String](
      PairSpec("left", StringCodec, Option(stringDefault)),
      PairSpec("right", StringCodec),
      _0
    )
    val t = obj.prepare(cellS2)
    val c = obj.present(Position("foo"), t.get).result
    c shouldBe Option(Cell(Position("foo"), getPairContent(stringDefault, "baz", PairCodec(StringCodec, StringCodec))))
  }

  it should "present with default values for right" in {
    val obj = GeneratePair[P, S, _0, Double, Double](
      PairSpec("left", DoubleCodec),
      PairSpec("right", DoubleCodec, Option(doubleDefault)),
      _0
    )
    val t = obj.prepare(cellD1)
    val c = obj.present(Position("foo"), t.get).result
    c shouldBe Option(Cell(Position("foo"), getPairContent(1.0, doubleDefault, PairCodec(DoubleCodec, DoubleCodec))))
  }

  it should "present empty with no default values" in {
    val obj = GeneratePair[P, S, _0, Double, Double](
      PairSpec("left", DoubleCodec),
      PairSpec("right", DoubleCodec),
      _0
    )
    val t = List.empty[(String, Either[Double, Double])]
    val c = obj.present(Position("foo"), t).result
      c shouldBe None
  }

  it should "present empty with both default values and no input" in {
    val obj = GeneratePair[P, S, _0, Double, Double](
      PairSpec("left", DoubleCodec, Option(doubleDefault)),
      PairSpec("right", DoubleCodec, Option(doubleDefault)),
      _0
    )
    val t = List.empty[(String, Either[Double, Double])]
    val c = obj.present(Position("foo"), t).result
      c shouldBe None
  }
}
