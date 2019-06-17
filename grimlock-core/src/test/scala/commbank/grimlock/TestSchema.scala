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

import commbank.grimlock.framework.encoding._
import commbank.grimlock.framework.metadata._

import java.text.SimpleDateFormat
import java.util.Date

class TestContinuousSchema extends TestGrimlock {
  "A ContinuousSchema" should "return its string representation" in {
    ContinuousSchema[Double]()
      .toShortString(DoubleCodec) shouldBe "continuous"
    ContinuousSchema[Double](-3.1415, 1.4142)
      .toShortString(DoubleCodec) shouldBe "continuous(min=-3.1415,max=1.4142)"
    ContinuousSchema[Double](-3.1415, 1.4142, 5, 4)
      .toShortString(DoubleCodec) shouldBe "continuous(min=-3.1415,max=1.4142,precision=5,scale=4)"
    ContinuousSchema[Double](Option(-3.1415), None, None, None)
      .toShortString(DoubleCodec) shouldBe "continuous(min=-3.1415)"
    ContinuousSchema[Double](None, Option(1.4142), None, None)
      .toShortString(DoubleCodec) shouldBe "continuous(max=1.4142)"
    ContinuousSchema[Double](None, None, Option(5), None)
      .toShortString(DoubleCodec) shouldBe "continuous(precision=5)"
    ContinuousSchema[Double](None, None, None, Option(4))
      .toShortString(DoubleCodec) shouldBe "continuous(scale=4)"
    ContinuousSchema[Double](Option(-3.1415), None, Option(5), None)
      .toShortString(DoubleCodec) shouldBe "continuous(min=-3.1415,precision=5)"
    ContinuousSchema[Double](Option(-3.1415), None, None, Option(4))
      .toShortString(DoubleCodec) shouldBe "continuous(min=-3.1415,scale=4)"
    ContinuousSchema[Double](None, Option(1.4142), Option(5), None)
      .toShortString(DoubleCodec) shouldBe "continuous(max=1.4142,precision=5)"
    ContinuousSchema[Double](None, Option(1.4142), None, Option(4))
      .toShortString(DoubleCodec) shouldBe "continuous(max=1.4142,scale=4)"
    ContinuousSchema[Double](None, None, Option(5), Option(4))
      .toShortString(DoubleCodec) shouldBe "continuous(precision=5,scale=4)"
    ContinuousSchema[Double](Option(-3.1415), None, Option(5), Option(4))
      .toShortString(DoubleCodec) shouldBe "continuous(min=-3.1415,precision=5,scale=4)"
    ContinuousSchema[Double](Option(-3.1415), Option(1.4142), None, Option(4))
      .toShortString(DoubleCodec) shouldBe "continuous(min=-3.1415,max=1.4142,scale=4)"
    ContinuousSchema[Double](Option(-3.1415), Option(1.4142), Option(5), None)
      .toShortString(DoubleCodec) shouldBe "continuous(min=-3.1415,max=1.4142,precision=5)"
    ContinuousSchema[Double](None, Option(1.4142), Option(5), Option(4))
      .toShortString(DoubleCodec) shouldBe "continuous(max=1.4142,precision=5,scale=4)"
  }

  it should "validate a correct value" in {
    ContinuousSchema[Double]().validate(DoubleValue(1)) shouldBe true
    ContinuousSchema[Double](-3.1415, 1.4142, 5, 4).validate(DoubleValue(1)) shouldBe true

    ContinuousSchema[Long]().validate(LongValue(1)) shouldBe true
    ContinuousSchema[Long](-1, 1, 5, 4).validate(LongValue(1)) shouldBe true
  }

  it should "not validate an incorrect value" in {
    ContinuousSchema[Double](-3.1415, 1.4142, 5, 4).validate(DoubleValue(4)) shouldBe false
    ContinuousSchema[Double](-3.1415, 1.4142, 5, 4).validate(DoubleValue(-4)) shouldBe false
    ContinuousSchema[Double](-3.1415, 1.4142, 5, 3).validate(DoubleValue(1.2345)) shouldBe false
    ContinuousSchema[Double](-3.1415, 14.142, 5, 4).validate(DoubleValue(12.3456)) shouldBe false

    ContinuousSchema[Long](-1, 1, 5, 4).validate(LongValue(4)) shouldBe false
    ContinuousSchema[Long](-1, 1, 5, 4).validate(LongValue(-4)) shouldBe false
    ContinuousSchema[Long](-1, 1234567, 5, 4).validate(LongValue(123456)) shouldBe false
  }

  it should "parse correctly" in {
    ContinuousSchema.fromShortString("continuous", DoubleCodec) shouldBe Option(ContinuousSchema[Double])
    ContinuousSchema.fromShortString("continuous()", DoubleCodec) shouldBe Option(ContinuousSchema[Double])
    ContinuousSchema.fromShortString(
      "continuous(min=-3.1415)",
      DoubleCodec
    ) shouldBe Option(ContinuousSchema[Double](Option(-3.1415), None, None, None))
    ContinuousSchema.fromShortString(
      "continuous(max=1.4142)",
      DoubleCodec
    ) shouldBe Option(ContinuousSchema[Double](None, Option(1.4142), None, None))
    ContinuousSchema.fromShortString(
      "continuous(precision=5)",
      DoubleCodec
    ) shouldBe Option(ContinuousSchema[Double](None, None, Option(5), None))
    ContinuousSchema.fromShortString(
      "continuous(scale=4)",
      DoubleCodec
    ) shouldBe Option(ContinuousSchema[Double](None, None, None, Option(4)))
    ContinuousSchema.fromShortString(
      "continuous(min=-3.1415,max=1.4142)",
      DoubleCodec
    ) shouldBe Option(ContinuousSchema[Double](-3.1415, 1.4142))
    ContinuousSchema.fromShortString(
      "continuous(min=-3.1415,precision=5)",
      DoubleCodec
    ) shouldBe Option(ContinuousSchema[Double](Option(-3.1415), None, Option(5), None))
    ContinuousSchema.fromShortString(
      "continuous(min=-3.1415,scale=4)",
      DoubleCodec
    ) shouldBe Option(ContinuousSchema[Double](Option(-3.1415), None, None, Option(4)))
    ContinuousSchema.fromShortString(
      "continuous(max=1.4142,precision=5)",
      DoubleCodec
    ) shouldBe Option(ContinuousSchema[Double](None, Option(1.4142), Option(5), None))
    ContinuousSchema.fromShortString(
      "continuous(max=1.4142,scale=4)",
      DoubleCodec
    ) shouldBe Option(ContinuousSchema[Double](None, Option(1.4142), None, Option(4)))
    ContinuousSchema.fromShortString(
      "continuous(precision=5,scale=4)",
      DoubleCodec
    ) shouldBe Option(ContinuousSchema[Double](None, None, Option(5), Option(4)))
    ContinuousSchema.fromShortString(
      "continuous(min=-3.1415,max=1.4142,scale=4)",
      DoubleCodec
    ) shouldBe Option(ContinuousSchema[Double](Option(-3.1415), Option(1.4142), None, Option(4)))
    ContinuousSchema.fromShortString(
      "continuous(min=-3.1415,max=1.4142,precision=5)",
      DoubleCodec
    ) shouldBe Option(ContinuousSchema[Double](Option(-3.1415), Option(1.4142), Option(5), None))
    ContinuousSchema.fromShortString(
      "continuous(min=-3.1415,precision=5,scale=4)",
      DoubleCodec
    ) shouldBe Option(ContinuousSchema[Double](Option(-3.1415), None, Option(5), Option(4)))
    ContinuousSchema.fromShortString(
      "continuous(max=1.4142,precision=5,scale=4)",
      DoubleCodec
    ) shouldBe Option(ContinuousSchema[Double](None, Option(1.4142), Option(5), Option(4)))
    ContinuousSchema.fromShortString(
      "continuous(min=-3.1415,max=1.4142,precision=5,scale=4)",
      DoubleCodec
    ) shouldBe Option(ContinuousSchema[Double](Option(-3.1415), Option(1.4142), Option(5), Option(4)))
  }

  it should "not parse an incorrect string" in {
    val strings = List(
      "continuous",
      "continuous()",
      "continuous(min=-1)",
      "continuous(max=1)",
      "continuous(precision=5)",
      "continuous(scale=4)",
      "continuous(min=-1,max=1)",
      "continuous(min=-1,precision=5)",
      "continuous(min=-1,scale=4)",
      "continuous(max=1,precision=5)",
      "continuous(max=1,scale=4)",
      "continuous(precision=5,scale=4)",
      "continuous(min=-1,max=1,scale=4)",
      "continuous(min=-1,max=1,precision=5)",
      "continuous(min=-1,precision=5,scale=4)",
      "continuous(max=1,precision=5,scale=4)",
      "continuous(min=-1,max=1,precision=5,scale=4)"
    )

    strings.foreach { case str =>
      (0 until str.length).foreach { case i =>
        ContinuousSchema.fromShortString(str.updated(i, "X").mkString, LongCodec) shouldBe None
      }
    }
  }
}

class TestDiscreteSchema extends TestGrimlock {
  "A DiscreteSchema" should "return its string representation" in {
    DiscreteSchema[Long]().toShortString(LongCodec) shouldBe "discrete"
    DiscreteSchema[Long](1).toShortString(LongCodec) shouldBe "discrete(step=1)"
    DiscreteSchema[Long](-1, 1).toShortString(LongCodec) shouldBe "discrete(min=-1,max=1)"
    DiscreteSchema[Long](-1, 1, 1).toShortString(LongCodec) shouldBe "discrete(min=-1,max=1,step=1)"
    DiscreteSchema[Long](Option(-1L), None, None).toShortString(LongCodec) shouldBe "discrete(min=-1)"
    DiscreteSchema[Long](None, Option(1L), None).toShortString(LongCodec) shouldBe "discrete(max=1)"
    DiscreteSchema[Long](Option(-1L), None, Option(1L)).toShortString(LongCodec) shouldBe "discrete(min=-1,step=1)"
    DiscreteSchema[Long](None, Option(1L), Option(1L)).toShortString(LongCodec) shouldBe "discrete(max=1,step=1)"
  }

  it should "validate a correct value" in {
    DiscreteSchema[Long]().validate(LongValue(1)) shouldBe true
    DiscreteSchema[Long](-1, 1, 1).validate(LongValue(1)) shouldBe true
    DiscreteSchema[Long](-4, 4, 2).validate(LongValue(2)) shouldBe true
  }

  it should "not validate an incorrect value" in {
    DiscreteSchema[Long](-1, 1, 1).validate(LongValue(4)) shouldBe false
    DiscreteSchema[Long](-1, 1, 1).validate(LongValue(-4)) shouldBe false
    DiscreteSchema[Long](-4, 4, 2).validate(LongValue(3)) shouldBe false
  }

  it should "parse correctly" in {
    DiscreteSchema.fromShortString("discrete", LongCodec) shouldBe Option(DiscreteSchema[Long])
    DiscreteSchema.fromShortString("discrete()", LongCodec) shouldBe Option(DiscreteSchema[Long])
    DiscreteSchema.fromShortString(
      "discrete(min=-1)",
      LongCodec
    ) shouldBe Option(DiscreteSchema[Long](Option(-1L), None, None))
    DiscreteSchema.fromShortString(
      "discrete(max=1)",
      LongCodec
    ) shouldBe Option(DiscreteSchema[Long](None, Option(1L), None))
    DiscreteSchema.fromShortString("discrete(step=1)", LongCodec) shouldBe Option(DiscreteSchema[Long](1))
    DiscreteSchema.fromShortString("discrete(min=-1,max=1)", LongCodec) shouldBe Option(DiscreteSchema[Long](-1, 1))
    DiscreteSchema.fromShortString(
      "discrete(min=-1,step=1)",
      LongCodec
    ) shouldBe Option(DiscreteSchema[Long](Option(-1L), None, Option(1L)))
    DiscreteSchema.fromShortString(
      "discrete(max=1,step=1)",
      LongCodec
    ) shouldBe Option(DiscreteSchema[Long](None, Option(1L), Option(1L)))
    DiscreteSchema.fromShortString(
      "discrete(min=-1,max=1,step=1)",
      LongCodec
    ) shouldBe Option(DiscreteSchema[Long](-1, 1, 1))
  }

  it should "not parse an incorrect string" in {
    val strings = List(
      "discrete",
      "discrete()",
      "discrete(min=-1)",
      "discrete(max=1)",
      "discrete(step=1)",
      "discrete(min=-1,max=1)",
      "discrete(min=-1,step=1)",
      "discrete(max=1,step=1)",
      "discrete(min=-1,max=1,step=1)"
    )

    strings.foreach { case str =>
      (0 until str.length).foreach { case i =>
        DiscreteSchema.fromShortString(str.updated(i, "X").mkString, LongCodec) shouldBe None
      }
    }
  }
}

class TestNominalSchema extends TestGrimlock {
  "A NominalSchema" should "return its string representation" in {
    NominalSchema[Double]().toShortString(DoubleCodec) shouldBe "nominal"
    NominalSchema[Double](Set[Double](1,2,3)).toShortString(DoubleCodec) shouldBe "nominal(set={1.0,2.0,3.0})"
    NominalSchema[Double]("\\d\\.\\d".r).toShortString(DoubleCodec) shouldBe "nominal(pattern=\\d\\.\\d)"

    NominalSchema[String]().toShortString(StringCodec) shouldBe "nominal"
    NominalSchema[String](Set("a","b","c",",","}")).toShortString(StringCodec) shouldBe "nominal(set={},a,\\,,b,c})"
    NominalSchema[String]("[abc,}]".r).toShortString(StringCodec) shouldBe "nominal(pattern=[abc,}])"
  }

  it should "validate a correct value" in {
    NominalSchema[Double]().validate(DoubleValue(1)) shouldBe true
    NominalSchema[Double](Set[Double](1,2,3)).validate(DoubleValue(1)) shouldBe true
    NominalSchema[Double]("\\d\\.\\d".r).validate(DoubleValue(1)) shouldBe true

    NominalSchema[String]().validate(StringValue("a")) shouldBe true
    NominalSchema[String](Set("a","b","c")).validate(StringValue("a")) shouldBe true
    NominalSchema[String]("[abc,}]".r).validate(StringValue("a")) shouldBe true
  }

  it should "not validate an incorrect value" in {
    NominalSchema[Double](Set[Double](1,2,3)).validate(DoubleValue(4)) shouldBe false
    NominalSchema[Double]("\\d\\.\\d".r).validate(DoubleValue(42)) shouldBe false

    NominalSchema[String](Set("a","b","c")).validate(StringValue("d")) shouldBe false
    NominalSchema[String]("[abc,}]".r).validate(StringValue("d")) shouldBe false
  }

  it should "parse correctly" in {
    NominalSchema.fromShortString("nominal", DoubleCodec) shouldBe Option(NominalSchema[Double]())
    NominalSchema.fromShortString("nominal()", DoubleCodec) shouldBe Option(NominalSchema[Double]())
    NominalSchema.fromShortString(
      "nominal(pattern=\\d\\.\\d)",
      DoubleCodec
    ).get.domain.right.get.pattern.toString shouldBe "\\d\\.\\d"
    NominalSchema.fromShortString(
      "nominal(set={-3.1415,1.4142,42})",
      DoubleCodec
    ) shouldBe Option(NominalSchema[Double](Set(-3.1415, 1.4142, 42)))

    NominalSchema.fromShortString("nominal", StringCodec) shouldBe Option(NominalSchema[String]())
    NominalSchema.fromShortString("nominal()", StringCodec) shouldBe Option(NominalSchema[String](Set[String]()))
    NominalSchema.fromShortString(
      "nominal(pattern=[abc,}])",
      StringCodec
    ).get.domain.right.get.pattern.toString shouldBe "[abc,}]"
    NominalSchema.fromShortString(
      "nominal(set={},a,a,\\,,,b,c})",
      StringCodec
    ) shouldBe Option(NominalSchema(Set[String]("a", "b", "c", ",", "}", "")))
    NominalSchema.fromShortString("nominal(set={,})", StringCodec) shouldBe Option(NominalSchema(Set[String]("")))
  }

  it should "not parse an incorrect string" in {
    NominalSchema.fromShortString("Xominal", DoubleCodec) shouldBe None
    NominalSchema.fromShortString("Xominal()", DoubleCodec) shouldBe None
    NominalSchema.fromShortString("nominalX)", DoubleCodec) shouldBe None
    NominalSchema.fromShortString("nominal(X", DoubleCodec) shouldBe None

    NominalSchema.fromShortString("Xominal(pattern=[123])", DoubleCodec) shouldBe None
    NominalSchema.fromShortString("nominalXpattern=[123])", DoubleCodec) shouldBe None
    NominalSchema.fromShortString("nominal(Xattern=[123])", DoubleCodec) shouldBe None
    NominalSchema.fromShortString("nominal(patternX[123])", DoubleCodec) shouldBe None
    NominalSchema.fromShortString("nominal(pattern=[123]X", DoubleCodec) shouldBe None

    NominalSchema.fromShortString("Xominal(set={1,2,3})", DoubleCodec) shouldBe None
    NominalSchema.fromShortString("nominalXset={1,2,3})", DoubleCodec) shouldBe None
    NominalSchema.fromShortString("nominal(Xet={1,2,3})", DoubleCodec) shouldBe None
    NominalSchema.fromShortString("nominal(setX{1,2,3})", DoubleCodec) shouldBe None
    NominalSchema.fromShortString("nominal(set=X1,2,3})", DoubleCodec) shouldBe None
    NominalSchema.fromShortString("nominal(set={1,X,3})", DoubleCodec) shouldBe None
    NominalSchema.fromShortString("nominal(set={1,2,3X)", DoubleCodec) shouldBe None
    NominalSchema.fromShortString("nominal(set={1,2,3}X", DoubleCodec) shouldBe None
    NominalSchema.fromShortString("nominal(set={})", DoubleCodec) shouldBe None
  }
}

class TestOrdinalSchema extends TestGrimlock {
  "A OrdinalSchema" should "return its string representation" in {
    OrdinalSchema[Double]().toShortString(DoubleCodec) shouldBe "ordinal"
    OrdinalSchema[Double](Set[Double](3,2,1)).toShortString(DoubleCodec) shouldBe "ordinal(set={1.0,2.0,3.0})"
    OrdinalSchema[Double]("\\d\\.\\d".r).toShortString(DoubleCodec) shouldBe "ordinal(pattern=\\d\\.\\d)"

    OrdinalSchema[String]().toShortString(StringCodec) shouldBe "ordinal"
    OrdinalSchema[String](Set("c","b","a")).toShortString(StringCodec) shouldBe "ordinal(set={a,b,c})"
    OrdinalSchema[String]("[abc,}]".r).toShortString(StringCodec) shouldBe "ordinal(pattern=[abc,}])"
  }

  it should "validate a correct value" in {
    OrdinalSchema[Double]().validate(DoubleValue(1)) shouldBe true
    OrdinalSchema[Double](Set[Double](1,2,3)).validate(DoubleValue(1)) shouldBe true
    OrdinalSchema[Double]("\\d\\.\\d".r).validate(DoubleValue(1)) shouldBe true

    OrdinalSchema[String]().validate(StringValue("a")) shouldBe true
    OrdinalSchema[String](Set("a","b","c")).validate(StringValue("a")) shouldBe true
    OrdinalSchema[String]("[abc,}]".r).validate(StringValue("a")) shouldBe true
  }

  it should "not validate an incorrect value" in {
    OrdinalSchema[Double](Set[Double](1,2,3)).validate(DoubleValue(4)) shouldBe false
    OrdinalSchema[Double]("\\d\\.\\d".r).validate(DoubleValue(42)) shouldBe false

    OrdinalSchema[String](Set("a","b","c")).validate(StringValue("d")) shouldBe false
    OrdinalSchema[String]("[abc,}]".r).validate(StringValue("d")) shouldBe false
  }

  it should "parse correctly" in {
    OrdinalSchema.fromShortString("ordinal", DoubleCodec) shouldBe Option(OrdinalSchema[Double]())
    OrdinalSchema.fromShortString("ordinal()", DoubleCodec) shouldBe Option(OrdinalSchema[Double]())
    OrdinalSchema.fromShortString(
      "ordinal(pattern=\\d\\.\\d)",
      DoubleCodec
    ).get.domain.right.get.pattern.toString shouldBe "\\d\\.\\d"
    OrdinalSchema.fromShortString(
      "ordinal(set={-3.1415,1.4142,42})",
      DoubleCodec
    ) shouldBe Option(OrdinalSchema[Double](Set(-3.1415, 1.4142, 42)))

    OrdinalSchema.fromShortString("ordinal", StringCodec) shouldBe Option(OrdinalSchema[String]())
    OrdinalSchema.fromShortString("ordinal()", StringCodec) shouldBe Option(OrdinalSchema[String](Set[String]()))
    OrdinalSchema.fromShortString(
      "ordinal(pattern=[abc,}])",
      StringCodec
    ).get.domain.right.get.pattern.toString shouldBe "[abc,}]"
    OrdinalSchema.fromShortString(
      "ordinal(set={},a,a,\\,,,b,c})",
      StringCodec
    ) shouldBe Option(OrdinalSchema(Set[String]("a", "b", "c", ",", "}", "")))
    OrdinalSchema.fromShortString("ordinal(set={,})", StringCodec) shouldBe Option(OrdinalSchema(Set[String]("")))
  }

  it should "not parse an incorrect string" in {
    OrdinalSchema.fromShortString("Xrdinal", DoubleCodec) shouldBe None
    OrdinalSchema.fromShortString("Xrdinal()", DoubleCodec) shouldBe None
    OrdinalSchema.fromShortString("ordinalX)", DoubleCodec) shouldBe None
    OrdinalSchema.fromShortString("ordinal(X", DoubleCodec) shouldBe None

    OrdinalSchema.fromShortString("Xrdinal(pattern=[123])", DoubleCodec) shouldBe None
    OrdinalSchema.fromShortString("ordinalXpattern=[123])", DoubleCodec) shouldBe None
    OrdinalSchema.fromShortString("ordinal(Xattern=[123])", DoubleCodec) shouldBe None
    OrdinalSchema.fromShortString("ordinal(patternX[123])", DoubleCodec) shouldBe None
    OrdinalSchema.fromShortString("ordinal(pattern=[123]X", DoubleCodec) shouldBe None

    OrdinalSchema.fromShortString("Xrdinal(set={1,2,3})", DoubleCodec) shouldBe None
    OrdinalSchema.fromShortString("ordinalXset={1,2,3})", DoubleCodec) shouldBe None
    OrdinalSchema.fromShortString("ordinal(Xet={1,2,3})", DoubleCodec) shouldBe None
    OrdinalSchema.fromShortString("ordinal(setX{1,2,3})", DoubleCodec) shouldBe None
    OrdinalSchema.fromShortString("ordinal(set=X1,2,3})", DoubleCodec) shouldBe None
    OrdinalSchema.fromShortString("ordinal(set={1,X,3})", DoubleCodec) shouldBe None
    OrdinalSchema.fromShortString("ordinal(set={1,2,3X)", DoubleCodec) shouldBe None
    OrdinalSchema.fromShortString("ordinal(set={1,2,3}X", DoubleCodec) shouldBe None
    OrdinalSchema.fromShortString("ordinal(set={})", DoubleCodec) shouldBe None
  }
}

class TestDateSchema extends TestGrimlock {
  val dcodec = DateCodec()
  val tcodec = DateCodec("yyyy-MM-dd hh:mm:ss")

  val dfmt = new SimpleDateFormat(dcodec.format)
  val tfmt = new SimpleDateFormat(tcodec.format)

  val date1 = dfmt.parse("2001-01-01")
  val date2 = dfmt.parse("2001-01-02")
  val date3 = dfmt.parse("2001-01-03")

  val time1 = tfmt.parse("2001-01-01 01:02:02")
  val time2 = tfmt.parse("2001-01-02 23:59:59")
  val time3 = tfmt.parse("2001-01-01 01:02:03")
  val time4 = tfmt.parse("2001-01-01 01:01:02")

  "A DateSchema" should "return its string representation" in {
    DateSchema[Date]().toShortString(dcodec) shouldBe "date"
    DateSchema[Date](date1, date2).toShortString(dcodec) shouldBe "date(min=2001-01-01,max=2001-01-02)"
    DateSchema[Date](Set(date1, date2)).toShortString(dcodec) shouldBe "date(set={2001-01-01,2001-01-02})"
    DateSchema[Date](Right((Option(date1), None))).toShortString(dcodec) shouldBe "date(min=2001-01-01)"
    DateSchema[Date](Right((None, Option(date2)))).toShortString(dcodec) shouldBe "date(max=2001-01-02)"
  }

  it should "validate a correct value" in {
    DateSchema[Date]().validate(DateValue(date1, dcodec)) shouldBe true
    DateSchema[Date](date1, date2).validate(DateValue(date1, dcodec)) shouldBe true
    DateSchema[Date](Set(date1, date2)).validate(DateValue(date1, dcodec)) shouldBe true

    DateSchema[Date]().validate(DateValue(time1, tcodec)) shouldBe true
    DateSchema[Date](time1, time2).validate(DateValue(time3, tcodec)) shouldBe true
    DateSchema[Date](Set(time1, time2)).validate(DateValue(time1, tcodec)) shouldBe true
  }

  it should "not validate an incorrect value" in {
    DateSchema[Date](date1, date2).validate(DateValue(date3, dcodec)) shouldBe false
    DateSchema[Date](Set(date1, date2)).validate(DateValue(date3, dcodec)) shouldBe false

    DateSchema[Date](time1, time2).validate(DateValue(time4, tcodec)) shouldBe false
    DateSchema[Date](Set(time1, time2)).validate(DateValue(time3, tcodec)) shouldBe false
  }

  it should "parse correctly" in {
    DateSchema.fromShortString("date", dcodec) shouldBe Option(DateSchema[Date]())
    DateSchema.fromShortString("date()", dcodec) shouldBe Option(DateSchema[Date]())
    DateSchema.fromShortString(
      "date(min=2001-01-01,max=2001-01-03)",
      dcodec
    ) shouldBe Option(DateSchema[Date](date1, date3))
    DateSchema.fromShortString(
      "date(set={2001-01-01,2001-01-02,2001-01-03})",
      dcodec
    ) shouldBe Option(DateSchema[Date](Set(date1, date2, date3)))
    DateSchema.fromShortString(
      "date(min=2001-01-01)",
      dcodec
    ) shouldBe Option(DateSchema[Date](Right((Option(date1), None))))
    DateSchema.fromShortString(
      "date(max=2001-01-03)",
      dcodec
    ) shouldBe Option(DateSchema[Date](Right((None, Option(date3)))))
  }

  it should "not parse an incorrect string" in {
    val strings = List(
      "date",
      "date()",
      "date(min=2001-01-01,max=2001-01-03)",
      "date(set={2001-01-01,2001-01-02,2001-01-03})",
      "date(min=2001-01-01)",
      "date(max=2001-01-03)"
    )

    strings.foreach { case str =>
      (0 until str.length).foreach { case i =>
        DateSchema.fromShortString(str.updated(i, "X").mkString, dcodec) shouldBe None
      }
    }
  }
}

class TestPairSchema extends TestGrimlock {
  val dfmt = new java.text.SimpleDateFormat("dd/MM/yyyy")
  val date2001 = dfmt.parse("01/01/2001")
  val one = BigDecimal(1.0)

  import commbank.grimlock.framework.environment.implicits.{ doubleToValue, longToValue, stringToValue }

  "A PairSchema" should "return its string representation" in {
    PairSchema(NominalSchema[String](Set("a","b","c",",","}")), ContinuousSchema[Double](-3.1415, 1.4142))
      .toShortString(PairCodec(StringCodec, DoubleCodec)) shouldBe
        "pair(left=nominal(set={},a,\\,,b,c}),right=continuous(min=-3.1415,max=1.4142))"
  }

  it should "validate a correct value" in {
    PairSchema(NominalSchema[String](), ContinuousSchema[Double]())
      .validate(PairValue(("abc", 1.0), PairCodec(StringCodec, DoubleCodec))) shouldBe true
  }

  it should "not validate an incorrect value" in {
    val obj = PairSchema(NominalSchema[String](Set("a", "b")), ContinuousSchema[Long](-1, 1, 5, 4))

    obj.validate(PairValue(("d", 1), PairCodec(StringCodec, LongCodec))) shouldBe false
    obj.validate(PairValue(("a", -4), PairCodec(StringCodec, LongCodec))) shouldBe false
  }

  it should "parse correctly" in {
    PairSchema.fromShortString(
      "pair(left=continuous(scale=4),right=ordinal(set={a,b}))",
      PairCodec(DoubleCodec, StringCodec),
      ContinuousSchema[Double](None, None, None, Option(4)),
      OrdinalSchema(Set[String]("a", "b"))
    ) shouldBe Option(
      PairSchema[Double, String](
        ContinuousSchema[Double](None, None, None, Option(4)),
        OrdinalSchema(Set[String]("a", "b"))
      )
    )
  }

  it should "not parse an incorrect string" in {
    PairSchema.fromShortString(
      "Xair",
      PairCodec(DoubleCodec, StringCodec),
      ContinuousSchema[Double](),
      OrdinalSchema[String]()
    ) shouldBe None
  }
}

