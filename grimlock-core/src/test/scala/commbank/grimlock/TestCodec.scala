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

import java.sql.Timestamp
import java.text.SimpleDateFormat

import commbank.grimlock.framework.encoding._
import commbank.grimlock.framework.metadata._

class TestDateCodec extends TestGrimlock {
  val codec = DateCodec("yyyy-MM-dd hh:ss:mm")

  val dfmt = new SimpleDateFormat(codec.format)

  val date1 = dfmt.parse("2001-01-01 01:01:01")
  val date2 = dfmt.parse("2002-01-01 01:01:01")

  "A DateCodec" should "have a name" in {
    codec.toShortString shouldBe "date(yyyy-MM-dd hh:ss:mm)"
  }

  it should "decode a correct value" in {
    codec.decode("2001-01-01 01:01:01") shouldBe Option(date1)
  }

  it should "not decode an incorrect value" in {
    codec.decode("a") shouldBe None
    codec.decode("1") shouldBe None
  }

  it should "encode a correct value" in {
    codec.encode(date1) shouldBe "2001-01-01 01:01:01"
  }

  it should "compare a correct value" in {
    codec.compare(date1, date2) shouldBe -1
    codec.compare(date1, date1) shouldBe 0
    codec.compare(date2, date1) shouldBe 1
  }

  it should "box correctly" in {
    codec.box(date1) shouldBe DateValue(date1, codec.format)
  }

  it should "return fields" in {
    codec.converters.size shouldBe 1

    codec.date.isDefined shouldBe true
    codec.integral.isEmpty shouldBe true
    codec.numeric.isEmpty shouldBe true
  }
}

class TestStringCodec extends TestGrimlock {
  "A StringCodec" should "have a name" in {
    StringCodec.toShortString shouldBe "string"
  }

  it should "decode a correct value" in {
    StringCodec.decode("abc") shouldBe Option("abc")
  }

  it should "encode a correct value" in {
    StringCodec.encode("abc") shouldBe "abc"
  }

  it should "compare a correct value" in {
    StringCodec.compare("abc", "bbc") shouldBe -1
    StringCodec.compare("abc", "abc") shouldBe 0
    StringCodec.compare("bbc", "abc") shouldBe 1
  }

  it should "box correctly" in {
    StringCodec.box("abc") shouldBe StringValue("abc")
  }

  it should "return fields" in {
    StringCodec.converters.size shouldBe 0

    StringCodec.date.isEmpty shouldBe true
    StringCodec.integral.isEmpty shouldBe true
    StringCodec.numeric.isEmpty shouldBe true
  }
}

class TestDecimalCodec extends TestGrimlock {
  val codec = DecimalCodec(5, 4)

  val bd1 = BigDecimal(3.1415)
  val bd2 = BigDecimal(42)

  "A DecimalCodec" should "have a name" in {
    codec.toShortString shouldBe "decimal(5,4)"
  }

  it should "decode a correct value" in {
    codec.decode("3.1415") shouldBe Option(bd1)
  }

  it should "not decode an incorrect value" in {
    codec.decode("a") shouldBe None
    codec.decode("3.14159265") shouldBe None
    codec.decode("314159.265") shouldBe None
  }

  it should "encode a correct value" in {
    codec.encode(bd1) shouldBe "3.1415"
  }

  it should "compare a correct value" in {
    codec.compare(bd1, bd2) shouldBe -1
    codec.compare(bd1, bd1) shouldBe 0
    codec.compare(bd2, bd1) shouldBe 1
  }

  it should "box correctly" in {
    codec.box(bd1) shouldBe DecimalValue(bd1, codec)
  }

  it should "return fields" in {
    codec.converters.size shouldBe 0

    codec.date.isEmpty shouldBe true
    codec.integral.isEmpty shouldBe true
    codec.numeric.isDefined shouldBe true
  }

  "A DecimalCodec without scale" should "have a type" in {
    DecimalCodec.fromShortString("decimal(10)") shouldBe Some(DecimalCodec(10, 0))
  }

  "A DecimalCodec with scale" should "have a type" in {
    DecimalCodec.fromShortString("decimal(10,3)") shouldBe Some(DecimalCodec(10, 3))
  }
}

class TestDoubleCodec extends TestGrimlock {
  "A DoubleCodec" should "have a name" in {
    DoubleCodec.toShortString shouldBe "double"
  }

  it should "decode a correct value" in {
    DoubleCodec.decode("3.14") shouldBe Option(3.14)
  }

  it should "not decode an incorrect value" in {
    DoubleCodec.decode("a") shouldBe None
    DoubleCodec.decode("2001-01-01") shouldBe None
  }

  it should "encode a correct value" in {
    DoubleCodec.encode(3.14) shouldBe "3.14"
  }

  it should "compare a correct value" in {
    DoubleCodec.compare(3.14, 4.14) shouldBe -1
    DoubleCodec.compare(3.14, 3.14) shouldBe 0
    DoubleCodec.compare(4.14, 3.14) shouldBe 1
  }

  it should "box correctly" in {
    DoubleCodec.box(3.14) shouldBe DoubleValue(3.14)
  }

  it should "return fields" in {
    DoubleCodec.converters.size shouldBe 0

    DoubleCodec.date.isEmpty shouldBe true
    DoubleCodec.integral.isEmpty shouldBe true
    DoubleCodec.numeric.isDefined shouldBe true
  }
}

class TestFloatCodec extends TestGrimlock {
  "A FloatCodec" should "have a name" in {
    FloatCodec.toShortString shouldBe "float"
  }

  it should "decode a correct value" in {
    FloatCodec.decode("3.14") shouldBe Option(3.14f)
    FloatCodec.decode("0.12345678901234") shouldBe Option(0.12345679f)
  }

  it should "not decode an incorrect value" in {
    FloatCodec.decode("a") shouldBe None
    FloatCodec.decode("2001-01-01") shouldBe None
  }

  it should "encode a correct value" in {
    FloatCodec.encode(3.14f) shouldBe "3.14"
  }

  it should "compare a correct value" in {
    FloatCodec.compare(3.14f, 4.14f) shouldBe -1
    FloatCodec.compare(3.14f, 3.14f) shouldBe 0
    FloatCodec.compare(4.14f, 3.14f) shouldBe 1
  }

  it should "box correctly" in {
    FloatCodec.box(3.14f) shouldBe FloatValue(3.14f)
  }

  it should "return fields" in {
    FloatCodec.converters.size shouldBe 1

    FloatCodec.date.isEmpty shouldBe true
    FloatCodec.integral.isEmpty shouldBe true
    FloatCodec.numeric.isDefined shouldBe true
  }
}

class TestIntCodec extends TestGrimlock {
  "A IntCodec" should "have a name" in {
    IntCodec.toShortString shouldBe "int"
  }

  it should "decode a correct value" in {
    IntCodec.decode("42") shouldBe Option(42)
  }

  it should "not decode an incorrect value" in {
    IntCodec.decode("a") shouldBe None
    IntCodec.decode("2001-01-01") shouldBe None
  }

  it should "encode a correct value" in {
    IntCodec.encode(42) shouldBe "42"
  }

  it should "compare a correct value" in {
    IntCodec.compare(3, 4) shouldBe -1
    IntCodec.compare(3, 3) shouldBe 0
    IntCodec.compare(4, 3) shouldBe 1
  }

  it should "box correctly" in {
    IntCodec.box(3) shouldBe IntValue(3)
  }

  it should "return fields" in {
    IntCodec.converters.size shouldBe 4

    IntCodec.date.isEmpty shouldBe true
    IntCodec.integral.isDefined shouldBe true
    IntCodec.numeric.isDefined shouldBe true
  }
}

class TestLongCodec extends TestGrimlock {
  "A LongCodec" should "have a name" in {
    LongCodec.toShortString shouldBe "long"
  }

  it should "decode a correct value" in {
    LongCodec.decode("42") shouldBe Option(42L)
  }

  it should "not decode an incorrect value" in {
    LongCodec.decode("a") shouldBe None
    LongCodec.decode("2001-01-01") shouldBe None
  }

  it should "encode a correct value" in {
    LongCodec.encode(42) shouldBe "42"
  }

  it should "compare a correct value" in {
    LongCodec.compare(3L, 4L) shouldBe -1
    LongCodec.compare(3L, 3L) shouldBe 0
    LongCodec.compare(4L, 3L) shouldBe 1
  }

  it should "box correctly" in {
    LongCodec.box(3) shouldBe LongValue(3)
  }

  it should "return fields" in {
    LongCodec.converters.size shouldBe 4

    LongCodec.date.isDefined shouldBe true
    LongCodec.integral.isDefined shouldBe true
    LongCodec.numeric.isDefined shouldBe true
  }
}

class TestBooleanCodec extends TestGrimlock {
  "A BooleanCodec" should "have a name" in {
    BooleanCodec.toShortString shouldBe "boolean"
  }

  it should "decode a correct value" in {
    BooleanCodec.decode("true") shouldBe Option(true)
    BooleanCodec.decode("false") shouldBe Option(false)
  }

  it should "not decode an incorrect value" in {
    BooleanCodec.decode("a") shouldBe None
    BooleanCodec.decode("2001-01-01") shouldBe None
  }

  it should "encode a correct value" in {
    BooleanCodec.encode(true) shouldBe "true"
  }

  it should "compare a correct value" in {
    BooleanCodec.compare(false, true) shouldBe -1
    BooleanCodec.compare(false, false) shouldBe 0
    BooleanCodec.compare(true, false) shouldBe 1
  }

  it should "box correctly" in {
    BooleanCodec.box(true) shouldBe BooleanValue(true)
  }

  it should "return fields" in {
    BooleanCodec.converters.size shouldBe 4

    BooleanCodec.date.isEmpty shouldBe true
    BooleanCodec.integral.isEmpty shouldBe true
    BooleanCodec.numeric.isEmpty shouldBe true
  }
}

class TestTimestampCodec extends TestGrimlock {
  val dfmt = new SimpleDateFormat("yyyy-MM-dd hh:ss:mm")

  val date1 = new Timestamp(dfmt.parse("2001-01-01 01:01:01").getTime)
  val date2 = new Timestamp(dfmt.parse("2002-01-01 01:01:01").getTime)

  "A DateCodec" should "have a name" in {
    TimestampCodec.toShortString shouldBe "timestamp"
  }

  it should "decode a correct value" in {
    TimestampCodec.decode("2001-01-01 01:01:01") shouldBe Option(date1)
  }

  it should "not decode an incorrect value" in {
    TimestampCodec.decode("a") shouldBe None
    TimestampCodec.decode("1") shouldBe None
  }

  it should "encode a correct value" in {
    TimestampCodec.encode(date1) shouldBe "2001-01-01 01:01:01.0"
  }

  it should "compare a correct value" in {
    TimestampCodec.compare(date1, date2) shouldBe -1
    TimestampCodec.compare(date1, date1) shouldBe 0
    TimestampCodec.compare(date2, date1) shouldBe 1
  }

  it should "box correctly" in {
    TimestampCodec.box(date1) shouldBe TimestampValue(date1)
  }

  it should "return fields" in {
    TimestampCodec.converters.size shouldBe 1

    TimestampCodec.date.isDefined shouldBe true
    TimestampCodec.integral.isEmpty shouldBe true
    TimestampCodec.numeric.isEmpty shouldBe true
  }
}

class TestBinaryCodec extends TestGrimlock {
  val one = "1".getBytes.head
  val two = "2".getBytes.head

  val bin = Array(one, two)

  "A BinaryCodec" should "have a name" in {
    BinaryCodec.toShortString shouldBe "binary"
  }

  it should "decode a correct value" in {
    BinaryCodec.decode("12").map(_.sameElements(bin)) shouldBe Option(true)
  }

  it should "encode a correct value" in {
    BinaryCodec.encode(bin) shouldBe "12"
  }

  it should "compare a correct value" in {
    BinaryCodec.compare(Array(one), bin) < 0 shouldBe true
    BinaryCodec.compare(Array(one, one), bin) < 0 shouldBe true
    BinaryCodec.compare(bin, bin) shouldBe 0
    BinaryCodec.compare(bin, Array(one)) > 0 shouldBe true
    BinaryCodec.compare(bin, Array(one, one)) > 0 shouldBe true
    BinaryCodec.compare(Array(one, one, one), bin) > 0 shouldBe true
  }

  it should "box correctly" in {
    BinaryCodec.box(bin) shouldBe BinaryValue(bin)
  }

  it should "return fields" in {
    BinaryCodec.converters.size shouldBe 0

    BinaryCodec.date.isEmpty shouldBe true
    BinaryCodec.integral.isEmpty shouldBe true
    BinaryCodec.numeric.isEmpty shouldBe true
  }
}

class TestTypeCodec extends TestGrimlock {
  "A TypeCodec" should "have a name" in {
    TypeCodec.toShortString shouldBe "type"
  }

  it should "decode a correct value" in {
    TypeCodec.decode("mixed") shouldBe Option(MixedType)
    TypeCodec.decode("categorical") shouldBe Option(CategoricalType)
    TypeCodec.decode("nominal") shouldBe Option(NominalType)
    TypeCodec.decode("ordinal") shouldBe Option(OrdinalType)
    TypeCodec.decode("numeric") shouldBe Option(NumericType)
    TypeCodec.decode("continuous") shouldBe Option(ContinuousType)
    TypeCodec.decode("discrete") shouldBe Option(DiscreteType)
    TypeCodec.decode("date") shouldBe Option(DateType)
  }

  it should "not decode an incorrect value" in {
    TypeCodec.decode("a") shouldBe None
    TypeCodec.decode("2001-01-01") shouldBe None
  }

  it should "encode a correct value" in {
    TypeCodec.encode(MixedType) shouldBe "mixed"
    TypeCodec.encode(CategoricalType) shouldBe "categorical"
    TypeCodec.encode(NominalType) shouldBe "nominal"
    TypeCodec.encode(OrdinalType) shouldBe "ordinal"
    TypeCodec.encode(NumericType) shouldBe "numeric"
    TypeCodec.encode(ContinuousType) shouldBe "continuous"
    TypeCodec.encode(DiscreteType) shouldBe "discrete"
    TypeCodec.encode(DateType) shouldBe "date"
  }

  it should "compare a correct value" in {
    TypeCodec.compare(DiscreteType, MixedType) < 0 shouldBe true
    TypeCodec.compare(DateType, DateType) shouldBe 0
    TypeCodec.compare(NominalType, ContinuousType) > 0 shouldBe true
  }

  it should "box correctly" in {
    TypeCodec.box(MixedType) shouldBe TypeValue(MixedType)
  }

  it should "return fields" in {
    TypeCodec.converters.size shouldBe 0

    TypeCodec.date.isEmpty shouldBe true
    TypeCodec.integral.isEmpty shouldBe true
    TypeCodec.numeric.isEmpty shouldBe true
  }
}

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
