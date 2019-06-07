// Copyright 2014,2015,2016,2017,2018,2019 Commonwealth Bank of Australia
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

package commbank.grimlock.framework.encoding

import commbank.grimlock.framework.metadata.Type

import java.sql.Timestamp
import java.util.Date

import scala.math.BigDecimal
import scala.reflect.{ classTag, ClassTag }
import scala.reflect.runtime.universe.{ typeTag, TypeTag }
import scala.util.matching.Regex

/** Trait for variable values. */
trait Value[T] {

  /** TypeTag of type of the Value. */
  protected val ttag: TypeTag[T]

  /** The codec used to encode/decode this value. */
  val codec: Codec[T]

  /** The encapsulated value. */
  val value: T

  /** Return value as `X` (if an appropriate converter exists), or `None` if the conversion is not supported. */
  def as[X : ClassTag: TypeTag]: Option[X] = {
    val ct = implicitly[ClassTag[X]]

    def cast(v: Any): Option[X] = v match {
      case ct(x) if ttag == typeTag[X] => Option(x)
      case _ => None
    }

    codec
      .converters
      .flatMap { case convert => cast(convert(value)) }
      .headOption
      .orElse(cast(value))
  }

  /**
   * Compare this value with a `T`.
   *
   * @param that The value to compare against.
   *
   * @return The returned value is < 0 iff this < that, 0 iff this = that, > 0 iff this > that.
   */
  def cmp(that: T): Int = codec.compare(value, that)

  /**
   * Compare this value with another.
   *
   * @param that The `Value` to compare against.
   *
   * @return If that can be compared to this, then an `Option` where the value is < 0 iff this < that,
   *         0 iff this = that, > 0 iff this > that. A `None` in all other case.
   */
  def cmp[V <% Value[_]](that: V): Option[Int]

  /**
   * Check for equality with `that`.
   *
   * @param that Value to compare against.
   */
  def equ[V <% Value[_]](that: V): Boolean = evaluate(that, Equal)

  /**
   * Check if `this` is greater or equal to `that`.
   *
   * @param that Value to compare against.
   *
   * @note If `that` is of a type that can't be compares to `this`, then the result is always `false`. This
   *       is the desired behaviour for the `Matrix.which` method (i.e. a filter).
   */
  def geq[V <% Value[_]](that: V): Boolean = evaluate(that, GreaterEqual)

  /**
   * Check if `this` is greater than `that`.
   *
   * @param that Value to compare against.
   *
   * @note If `that` is of a type that can't be compares to `this`, then the result is always `false`. This
   *       is the desired behaviour for the `Matrix.which` method (i.e. a filter).
   */
  def gtr[V <% Value[_]](that: V): Boolean = evaluate(that, Greater)

  /**
   * Check if `this` is less or equal to `that`.
   *
   * @param that Value to compare against.
   *
   * @note If `that` is of a type that can't be compares to `this`, then the result is always `false`. This
   *       is the desired behaviour for the `Matrix.which` method (i.e. a filter).
   */
  def leq[V <% Value[_]](that: V): Boolean = evaluate(that, LessEqual)

  /**
   * Check for for match with `that` regular expression.
   *
   * @param that Regular expression to match against.
   *
   * @note This always applies `toShortString` method before matching.
   */
  def like(that: Regex): Boolean = that.pattern.matcher(this.toShortString).matches

  /**
   * Check if `this` is less than `that`.
   *
   * @param that Value to compare against.
   *
   * @note If `that` is of a type that can't be compares to `this`, then the result is always `false`. This
   *       is the desired behaviour for the `Matrix.which` method (i.e. a filter).
   */
  def lss[V <% Value[_]](that: V): Boolean = evaluate(that, Less)

  /**
   * Check for in-equality with `that`.
   *
   * @param that Value to compare against.
   */
  def neq[V <% Value[_]](that: V): Boolean = !(this equ that)

  /** Return a consise (terse) string representation of a value. */
  def toShortString: String = codec.encode(value)

  private def evaluate(that: Value[_], op: CompareResult): Boolean = cmp(that) match {
    case Some(0) => (op == Equal) || (op == GreaterEqual) || (op == LessEqual)
    case Some(x) if (x > 0) => (op == Greater) || (op == GreaterEqual)
    case Some(x) if (x < 0) => (op == Less) || (op == LessEqual)
    case _ => false
  }
}

/** Compantion object to the `Value` trait. */
object Value {
  /** Type alias for value boxing. */
  type Box[T] = (T) => Value[T]

  /**
   * Concatenates the string representation of two values.
   *
   * @param separator Separator to use between the strings.
   *
   * @return A function that concatenates values as a string.
   */
  def concatenate[X <: Value[_], Y <: Value[_]](separator: String): (X, Y) => String = (x, y) =>
    x.toShortString + separator + y.toShortString

  /**
   * Parse a value from string.
   *
   * @param str   The string to parse.
   * @param codec The codec to decode with.
   *
   * @return A `Option[codec.V]` if successful, `None` otherwise.
   */
  def fromShortString[T](str: String, codec: Codec[T]): Option[Value[T]] = codec.decode(str).map(t => codec.box(t))

  /**
   * Define an ordering between 2 values.
   *
   * @param ascending Indicator if ordering should be ascending or descending.
   */
  def ordering[V <: Value[_]](ascending: Boolean = true): Ordering[V] = new Ordering[V] {
    def compare(x: V, y: V): Int = (if (ascending) 1 else -1) * x
      .cmp(y)
      .getOrElse(throw new Exception("Different types should not be possible"))
  }
}

/**
 * Value for when the data is of type `Array[Byte]`.
 *
 * @param value An `Array[Byte]`.
 * @param codec The codec used for encoding/decoding `value`.
 */
case class BinaryValue(value: Array[Byte], codec: Codec[Array[Byte]] = BinaryCodec) extends Value[Array[Byte]] {
  def cmp[V <% Value[_]](that: V): Option[Int] = that.as[Array[Byte]].map(ab => cmp(ab))

  protected val ttag: TypeTag[Array[Byte]] = typeTag[Array[Byte]]
}

/** Companion object to `BinaryValue` case class. */
object BinaryValue {
  /** `unapply` method for pattern matching. */
  def unapply(value: Value[_]): Option[Array[Byte]] = classTag[Array[Byte]].unapply(value.value)
}

/**
 * Value for when the data is of type `Boolean`.
 *
 * @param value A `Boolean`.
 * @param codec The codec used for encoding/decoding `value`.
 */
case class BooleanValue(value: Boolean, codec: Codec[Boolean] = BooleanCodec) extends Value[Boolean] {
  def cmp[V <% Value[_]](that: V): Option[Int] = that.as[Boolean].map(b => cmp(b))

  protected val ttag: TypeTag[Boolean] = typeTag[Boolean]
}

/** Companion object to `BooleanValue` case class. */
object BooleanValue {
  /** `unapply` method for pattern matching. */
  def unapply(value: Value[_]): Option[Boolean] = ClassTag.Boolean.unapply(value.value)
}

/**
 * Value for when the data is of type `java.util.Date`
 *
 * @param value A `java.util.Date`.
 * @param codec The codec used for encoding/decoding `value`.
 */
case class DateValue(value: Date, codec: Codec[Date] = DateCodec()) extends Value[Date] {
  def cmp[V <% Value[_]](that: V): Option[Int] = that.as[Date].map(d => cmp(d))

  protected val ttag: TypeTag[Date] = typeTag[Date]
}

/** Companion object to `DateValue` case class. */
object DateValue {
  /** Convenience constructure that returns a `DateValue` from a date and format string. */
  def apply(value: Date, format: String): DateValue = DateValue(value, DateCodec(format))

  /** `unapply` method for pattern matching. */
  def unapply(value: Value[_]): Option[Date] = classTag[Date].unapply(value.value)
}

/**
 * Value for when the data is of type `BigDecimal`.
 *
 * @param value A `BigDecimal`.
 * @param codec The codec used for encoding/decoding `value`.
 */
case class DecimalValue(value: BigDecimal, codec: Codec[BigDecimal]) extends Value[BigDecimal] {
  def cmp[V <% Value[_]](that: V): Option[Int] = that.as[BigDecimal].map(bd => cmp(bd))

  protected val ttag: TypeTag[BigDecimal] = typeTag[BigDecimal]
}

/** Companion object to `DecimalValue` case class. */
object DecimalValue {
  /** `unapply` method for pattern matching. */
  def unapply(value: Value[_]): Option[BigDecimal] = classTag[BigDecimal].unapply(value.value)
}

/**
 * Value for when the data is of type `Double`.
 *
 * @param value A `Double`.
 * @param codec The codec used for encoding/decoding `value`.
 */
case class DoubleValue(value: Double, codec: Codec[Double] = DoubleCodec) extends Value[Double] {
  def cmp[V <% Value[_]](that: V): Option[Int] = that.as[Double].map(d => cmp(d))

  protected val ttag: TypeTag[Double] = typeTag[Double]
}

/** Companion object to `DoubleValue` case class. */
object DoubleValue {
  /** `unapply` method for pattern matching. */
  def unapply(value: Value[_]): Option[Double] = ClassTag.Double.unapply(value.value)
}

/**
 * Value for when the data is of type `Float`.
 *
 * @param value A `Float`.
 * @param codec The codec used for encoding/decoding `value`.
 */
case class FloatValue(value: Float, codec: Codec[Float] = FloatCodec) extends Value[Float] {
  def cmp[V <% Value[_]](that: V): Option[Int] = that.as[Float].map(d => cmp(d))

  protected val ttag: TypeTag[Float] = typeTag[Float]
}

/** Companion object to `FloatValue` case class. */
object FloatValue {
  /** `unapply` method for pattern matching. */
  def unapply(value: Value[_]): Option[Float] = ClassTag.Float.unapply(value.value)
}

/**
 * Value for when the data is of type `Int`.
 *
 * @param value A `Int`.
 * @param codec The codec used for encoding/decoding `value`.
 */
case class IntValue(value: Int, codec: Codec[Int] = IntCodec) extends Value[Int] {
  def cmp[V <% Value[_]](that: V): Option[Int] = that
    .as[Int]
    .map(i => super.cmp(i))
    .orElse(that.as[Long].map(l => super.cmp(l.toInt)))
    .orElse(that.as[Double].map(d => super.cmp(if (d > value) math.ceil(d).toInt else math.floor(d).toInt)))

  protected val ttag: TypeTag[Int] = typeTag[Int]
}

/** Companion object to `IntValue` case class. */
object IntValue {
  /** `unapply` method for pattern matching. */
  def unapply(value: Value[_]): Option[Int] = ClassTag.Int.unapply(value.value)
}

/**
 * Value for when the data is of type `Long`.
 *
 * @param value A `Long`.
 * @param codec The codec used for encoding/decoding `value`.
 */
case class LongValue(value: Long, codec: Codec[Long] = LongCodec) extends Value[Long] {
  def cmp[V <% Value[_]](that: V): Option[Int] = that
    .as[Long]
    .map(l => super.cmp(l))
    .orElse(that.as[Double].map(d => super.cmp(if (d > value) math.ceil(d).toLong else math.floor(d).toLong)))

  protected val ttag: TypeTag[Long] = typeTag[Long]
}

/** Companion object to `LongValue` case class. */
object LongValue {
  /** `unapply` method for pattern matching. */
  def unapply(value: Value[_]): Option[Long] = ClassTag.Long.unapply(value.value)
}

/**
 * Value for when the data is of type `String`.
 *
 * @param value A `String`.
 * @param codec The codec used for encoding/decoding `value`.
 */
case class StringValue(value: String, codec: Codec[String] = StringCodec) extends Value[String] {
  def cmp[V <% Value[_]](that: V): Option[Int] = that.as[String].map(s => cmp(s))

  protected val ttag: TypeTag[String] = typeTag[String]
}

/** Companion object to `StringValue` case class. */
object StringValue {
  /** `unapply` method for pattern matching. */
  def unapply(value: Value[_]): Option[String] = classTag[String].unapply(value.value)
}

/**
 * Value for when the data is of type `Timestamp`.
 *
 * @param value A `Timestamp`.
 * @param codec The codec used for encoding/decoding `value`.
 */
case class TimestampValue(value: Timestamp, codec: Codec[Timestamp] = TimestampCodec) extends Value[Timestamp] {
  def cmp[V <% Value[_]](that: V): Option[Int] = that.as[Timestamp].map(t => cmp(t))

  protected val ttag: TypeTag[Timestamp] = typeTag[Timestamp]
}

/** Companion object to `TimestampValue` case class. */
object TimestampValue {
  /** `unapply` method for pattern matching. */
  def unapply(value: Value[_]): Option[Timestamp] = classTag[Timestamp].unapply(value.value)
}

/**
 * Value for when the data is of type `Type`.
 *
 * @param value A `Type`.
 * @param codec The codec used for encoding/decoding `value`.
 */
case class TypeValue(value: Type, codec: Codec[Type] = TypeCodec) extends Value[Type] {
  def cmp[V <% Value[_]](that: V): Option[Int] = that.as[Type].map(t => cmp(t))

  protected val ttag: TypeTag[Type] = typeTag[Type]
}

/** Companion object to `TypeValue` case class. */
object TypeValue {
  /** `unapply` method for pattern matching. */
  def unapply(value: Value[_]): Option[Type] = classTag[Type].unapply(value.value)
}

/** Hetrogeneous comparison results. */
sealed private trait CompareResult
private case object GreaterEqual extends CompareResult
private case object Greater extends CompareResult
private case object Equal extends CompareResult
private case object Less extends CompareResult
private case object LessEqual extends CompareResult

