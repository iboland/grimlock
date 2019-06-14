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

package commbank.grimlock.framework.metadata

import commbank.grimlock.framework.encoding.{ Codec, IntCodec, PairCodec, Value }

import java.util.Date

import scala.util.matching.Regex

import shapeless.{ :+:, CNil, Coproduct }
import shapeless.ops.coproduct.Inject

/** Trait for variable schemas. */
trait Schema[T] {
  /** The type of variable. */
  val classification: Type

  /**
   * Return a consise (terse) string representation of a schema.
   *
   * @param codec The codec used to encode this schema's data.
   */
  def toShortString(codec: Codec[T]): String = classification.toShortString + round(paramString(codec))

  /**
   * Validates if a value confirms to this schema.
   *
   * @param value The value to validate.
   *
   * @return True is the value confirms to this schema, false otherwise.
   */
  def validate(value: Value[T]): Boolean

  protected def paramString(codec: Codec[T]): String = ""

  private def round(str: String): String = if (str.isEmpty) str else "(" + str + ")"
}

/** Companion object to the `Schema` trait. */
object Schema {
  /** Type for a default Codec co-product when parsing Schemas from string. */
  type DefaultSchemas[T] = ContinuousSchema[T] :+:
    DateSchema[T] :+:
    DiscreteSchema[T] :+:
    NominalSchema[T] :+:
    OrdinalSchema[T] :+:
    CNil

  /** Type that captures all constraints for parsing default Schemas from string. */
  trait TextParseConstraints[C <: Coproduct, T] extends java.io.Serializable {
    implicit val asContinuous: Inject[C, ContinuousSchema[T]]
    implicit val asDate: Inject[C, DateSchema[T]]
    implicit val asDiscrete: Inject[C, DiscreteSchema[T]]
    implicit val asNominal: Inject[C, NominalSchema[T]]
    implicit val asOrdinal: Inject[C, OrdinalSchema[T]]
  }

  /** Implicit meeting text parsing constraints for the default Schemas. */
  implicit def schemaTextParseConstraints[
    C <: Coproduct,
    T
  ](implicit
    ev1: Inject[C, ContinuousSchema[T]],
    ev2: Inject[C, DateSchema[T]],
    ev3: Inject[C, DiscreteSchema[T]],
    ev4: Inject[C, NominalSchema[T]],
    ev5: Inject[C, OrdinalSchema[T]]
  ): TextParseConstraints[C, T] = new TextParseConstraints[C, T] {
    implicit val asContinuous = ev1
    implicit val asDate = ev2
    implicit val asDiscrete = ev3
    implicit val asNominal = ev4
    implicit val asOrdinal = ev5
  }

  /**
   * Parse a schema from a string.
   *
   * @param str   String from which to parse the schema.
   * @param codec Codec with which to decode the data.
   *
   * @return A `Some[C]` in case of success, `None` otherwise.
   */
  def fromShortString[
    C <: Coproduct,
    T
  ](
    str: String,
    codec: Codec[T]
  )(implicit
    ev: TextParseConstraints[C, T]
  ): Option[C] = {
    import ev._

    str match {
      case ContinuousSchema.Pattern() => ContinuousSchema.fromShortString(str, codec).map(Coproduct(_))
      case DateSchema.Pattern() => DateSchema.fromShortString(str, codec).map(Coproduct(_))
      case DiscreteSchema.Pattern() => DiscreteSchema.fromShortString(str, codec).map(Coproduct(_))
      case NominalSchema.Pattern() => NominalSchema.fromShortString(str, codec).map(Coproduct(_))
      case OrdinalSchema.Pattern() => OrdinalSchema.fromShortString(str, codec).map(Coproduct(_))
      case _ => None
    }
  }
}

/** Trait for schemas for numerical variables. */
trait NumericalSchema[T] extends Schema[T] {
  protected def validateRange(
    value: Value[T],
    min: Option[T],
    max: Option[T]
  )(implicit
    ev: Numeric[T]
  ): Boolean = min.fold(true)(t => value.cmp(t) >= 0) && max.fold(true)(t => value.cmp(t) <= 0)
}

/**
 * Schema for continuous variables.
 *
 * @param min       The minimum value.
 * @param max       The maximum value.
 * @param precision The maximum precision.
 * @param scale     The maximum scale.
 */
case class ContinuousSchema[
  T : Numeric
](
  min: Option[T],
  max: Option[T],
  precision: Option[Int],
  scale: Option[Int]
) extends NumericalSchema[T] {
  val classification = ContinuousType

  def validate(value: Value[T]): Boolean = {
    val ev = implicitly[Numeric[T]]

    import ev.mkNumericOps
    import scala.math.BigDecimal.double2bigDecimal

    validateRange(value, min, max) &&
      precision.fold(true)(p => value.value.toDouble.precision <= p) &&
      scale.fold(true)(s => value.value.toDouble.scale <= s)
  }

  override protected def paramString(codec: Codec[T]): String = SchemaParameters.writeRange(min, max, codec, extra)

  private val extra = List(("precision", precision.map(_.toString)), ("scale", scale.map(_.toString)))
}

/** Companion object to `ContinuousSchema` case class. */
object ContinuousSchema {
  /** Pattern for matching short string continuous schema without arguments. */
  val Pattern = s"${ContinuousType.name}.*".r

  /** Construct a continuous schema with unbounded range. */
  def apply[T : Numeric](): ContinuousSchema[T] = ContinuousSchema(None, None, None, None)

  /**
   * Construct a continuous schema with bounded range.
   *
   * @param min The minimum value.
   * @param max The maximum value.
   */
  def apply[T : Numeric](min: T, max: T): ContinuousSchema[T] = ContinuousSchema(Option(min), Option(max), None, None)

  /**
   * Construct a continuous schema with bounded range, precision and scale.
   *
   * @param min       The minimum value.
   * @param max       The maximum value.
   * @param precision The maximum number of digits.
   * @param scale     The maximum number of decimal places.
   */
  def apply[
    T : Numeric
  ](
    min: T,
    max: T,
    precision: Int,
    scale: Int
  ): ContinuousSchema[T] = ContinuousSchema(Option(min), Option(max), Option(precision), Option(scale))

  /**
   * Parse a continuous schema from components.
   *
   * @param min       The minimum value string to parse.
   * @param max       The maximum value string to parse.
   * @param precision The maximum precision string to parse.
   * @param scale     The maximum scale string to parse.
   * @param codec     The codec to parse with.
   *
   * @return A `Some[ContinuousSchema]` if successful, `None` otherwise.
   */
  def fromComponents[
    T
  ](
    min: String,
    max: String,
    precision: String,
    scale: String,
    codec: Codec[T]
  ): Option[ContinuousSchema[T]] = for {
    low <- SchemaParameters.parseWrapped(codec, min)
    upp <- SchemaParameters.parseWrapped(codec, max)
    pre <- SchemaParameters.parseWrapped(IntCodec, precision)
    scl <- SchemaParameters.parseWrapped(IntCodec, scale)
    num <- codec.numeric
  } yield ContinuousSchema(low, upp, pre, scl)(num)

  /**
   * Parse a continuous schema from string.
   *
   * @param str   The string to parse.
   * @param codec The codec to parse with.
   *
   * @return A `Some[ContinuousSchema]` if successful, `None` otherwise.
   */
  def fromShortString[T](str: String, codec: Codec[T]): Option[ContinuousSchema[T]] = {
    val components = str match {
      case PatternEmpty() => Option(("", "", "", ""))
      case PatternMax(max) => Option(("", max, "", ""))
      case PatternMaxPrecision(max, pre) => Option(("", max, pre, ""))
      case PatternMaxScale(max, scl) => Option(("", max, "", scl))
      case PatternMaxPrecisionScale(max, pre, scl) => Option(("", max, pre, scl))
      case PatternMin(min) => Option((min, "", "", ""))
      case PatternMinPrecision(min, pre) => Option((min, "", pre, ""))
      case PatternMinScale(min, scl) => Option((min, "", "", scl))
      case PatternMinPrecisionScale(min, pre, scl) => Option((min, "", pre, scl))
      case PatternName() => Option(("", "", "", ""))
      case PatternPrecision(pre) => Option(("", "", pre, ""))
      case PatternPrecisionScale(pre, scl) => Option(("", "", pre, scl))
      case PatternRange(min, max) => Option((min, max, "", ""))
      case PatternRangePrecision(min, max, pre) => Option((min, max, pre, ""))
      case PatternRangePrecisionScale(min, max, pre, scl) => Option((min, max, pre, scl))
      case PatternRangeScale(min, max, scl) => Option((min, max, "", scl))
      case PatternScale(scl) => Option(("", "", "", scl))
      case _ => None
    }

    components.flatMap { case (min, max, pre, scl) => fromComponents(min, max, pre, scl, codec) }
  }

  /** Pattern for matching short string continuous schema without arguments. */
  private val PatternEmpty = s"${ContinuousType.name}\\(\\)".r

  /** Pattern for matching short string continuous schema with maximum value. */
  private val PatternMax = s"${ContinuousType.name}\\(max=(-?\\d+\\.?\\d*)\\)".r

  /** Pattern for matching short string continuous schema with maximum value and precision. */
  private val PatternMaxPrecision = s"${ContinuousType.name}\\(max=(-?\\d+\\.?\\d*),precision=(\\d+)\\)".r

  /** Pattern for matching short string continuous schema with maximum value and scale. */
  private val PatternMaxScale = s"${ContinuousType.name}\\(max=(-?\\d+\\.?\\d*),scale=(\\d+)\\)".r

  /** Pattern for matching short string continuous schema with maximum value, precision and scale. */
  private val PatternMaxPrecisionScale =
    s"${ContinuousType.name}\\(max=(-?\\d+\\.?\\d*),precision=(\\d+),scale=(\\d+)\\)".r

  /** Pattern for matching short string continuous schema with minimum value. */
  private val PatternMin = s"${ContinuousType.name}\\(min=(-?\\d+\\.?\\d*)\\)".r

  /** Pattern for matching short string continuous schema with minimum value and precision. */
  private val PatternMinPrecision = s"${ContinuousType.name}\\(min=(-?\\d+\\.?\\d*),precision=(\\d+)\\)".r

  /** Pattern for matching short string continuous schema with minimum value and scale. */
  private val PatternMinScale = s"${ContinuousType.name}\\(min=(-?\\d+\\.?\\d*),scale=(\\d+)\\)".r

  /** Pattern for matching short string continuous schema with minimum value, precision and scale. */
  private val PatternMinPrecisionScale =
    s"${ContinuousType.name}\\(min=(-?\\d+\\.?\\d*),precision=(\\d+),scale=(\\d+)\\)".r

  /** Pattern for matching short string continuous schema. */
  private val PatternName = ContinuousType.name.r

  /** Pattern for matching short string continuous schema with precision. */
  private val PatternPrecision = s"${ContinuousType.name}\\(precision=(\\d+)\\)".r

  /** Pattern for matching short string continuous schema with precision and scale. */
  private val PatternPrecisionScale = s"${ContinuousType.name}\\(precision=(\\d+),scale=(\\d+)\\)".r

  /** Pattern for matching short string continuous schema with range. */
  private val PatternRange = s"${ContinuousType.name}\\(min=(-?\\d+\\.?\\d*),max=(-?\\d+\\.?\\d*)\\)".r

  /** Pattern for matching short string continuous schema with range and precision. */
  private val PatternRangePrecision =
    s"${ContinuousType.name}\\(min=(-?\\d+\\.?\\d*),max=(-?\\d+\\.?\\d*),precision=(\\d+)\\)".r

  /** Pattern for matching short string continuous schema with range, precision and scale. */
  private val PatternRangePrecisionScale =
    s"${ContinuousType.name}\\(min=(-?\\d+\\.?\\d*),max=(-?\\d+\\.?\\d*),precision=(\\d+),scale=(\\d+)\\)".r

  /** Pattern for matching short string continuous schema with range and scale. */
  private val PatternRangeScale =
    s"${ContinuousType.name}\\(min=(-?\\d+\\.?\\d*),max=(-?\\d+\\.?\\d*),scale=(\\d+)\\)".r

  /** Pattern for matching short string continuous schema with scale. */
  private val PatternScale = s"${ContinuousType.name}\\(scale=(\\d+)\\)".r
}

/**
 * Schema for discrete variables.
 *
 * @param min  The minimum value.
 * @param max  The maximum value.
 * @param step The step size.
 */
case class DiscreteSchema[T : Integral](min: Option[T], max: Option[T], step: Option[T]) extends NumericalSchema[T] {
  val classification = DiscreteType

  def validate(value: Value[T]): Boolean = {
    val ev = implicitly[Integral[T]]

    import ev.mkNumericOps

    validateRange(value, min, max) && step.fold(true)(s => value.value % s == 0)
  }

  override protected def paramString(
    codec: Codec[T]
  ): String = SchemaParameters.writeRange(min, max, codec, List(("step", step.map(_.toString))))
}

/** Companion object to `DiscreteSchema` case class. */
object DiscreteSchema {
  /** Pattern for matching short string discrete schema without arguments. */
  val Pattern = s"${DiscreteType.name}.*".r

  /** Construct a discrete schema with unbounded range and no step size. */
  def apply[T : Integral](): DiscreteSchema[T] = DiscreteSchema(None, None, None)

  /**
   * Construct a discrete schema with step size.
   *
   * @param step The step size.
   */
  def apply[T : Integral](step: T): DiscreteSchema[T] = DiscreteSchema(None, None, Option(step))

  /**
   * Construct a discrete schema with bounded range.
   *
   * @param min The minimum value.
   * @param max The maximum value.
   */
  def apply[T : Integral](min: T, max: T): DiscreteSchema[T] = DiscreteSchema(Option(min), Option(max), None)

  /**
   * Construct a discrete schema with bounded range and step size.
   *
   * @param min  The minimum value.
   * @param max  The maximum value.
   * @param step The step size.
   */
  def apply[
    T : Integral
  ](
    min: T,
    max: T,
    step: T
  ): DiscreteSchema[T] = DiscreteSchema(Option(min), Option(max), Option(step))

  /**
   * Parse a discrete schema from components.
   *
   * @param min   The minimum value string to parse.
   * @param max   The maximum value string to parse.
   * @param step  The step size string to parse.
   * @param codec The codec to parse with.
   *
   * @return A `Some[DiscreteSchema]` if successful, `None` otherwise.
   */
  def fromComponents[T](min: String, max: String, step: String, codec: Codec[T]): Option[DiscreteSchema[T]] = for {
    low <- SchemaParameters.parseWrapped(codec, min)
    upp <- SchemaParameters.parseWrapped(codec, max)
    stp <- SchemaParameters.parseWrapped(codec, step)
    int <- codec.integral
  } yield DiscreteSchema(low, upp, stp)(int)

  /**
   * Parse a discrete schema from string.
   *
   * @param str   The string to parse.
   * @param codec The codec to parse with.
   *
   * @return A `Some[DiscreteSchema]` if successful, `None` otherwise.
   */
  def fromShortString[T](str: String, codec: Codec[T]): Option[DiscreteSchema[T]] = {
    val components = str match {
      case PatternEmpty() => Option(("", "", ""))
      case PatternMax(max) => Option(("", max, ""))
      case PatternMaxStep(max, step) => Option(("", max, step))
      case PatternMin(min) => Option((min, "", ""))
      case PatternMinStep(min, step) => Option((min, "", step))
      case PatternName() => Option(("", "", ""))
      case PatternRange(min, max) => Option((min, max, ""))
      case PatternRangeStep(min, max, step) => Option((min, max, step))
      case PatternStep(step) => Option(("", "", step))
      case _ => None
    }

    components.flatMap { case (min, max, stp) => fromComponents(min, max, stp, codec) }
  }

  /** Pattern for matching short string discrete schema without arguments. */
  private val PatternEmpty = s"${DiscreteType.name}\\(\\)".r

  /** Pattern for matching short string discrete schema with maximum value. */
  private val PatternMax = s"${DiscreteType.name}\\(max=(-?\\d+)\\)".r

  /** Pattern for matching short string discrete schema with maximum value and step size. */
  private val PatternMaxStep = s"${DiscreteType.name}\\(max=(-?\\d+),step=(\\d+)\\)".r

  /** Pattern for matching short string discrete schema with minimum value. */
  private val PatternMin = s"${DiscreteType.name}\\(min=(-?\\d+)\\)".r

  /** Pattern for matching short string discrete schema with minimum value and step size. */
  private val PatternMinStep = s"${DiscreteType.name}\\(min=(-?\\d+),step=(\\d+)\\)".r

  /** Pattern for matching short string discrete schema. */
  private val PatternName = DiscreteType.name.r

  /** Pattern for matching short string discrete schema with range. */
  private val PatternRange = s"${DiscreteType.name}\\(min=(-?\\d+),max=(-?\\d+)\\)".r

  /** Pattern for matching short string discrete schema with range and step size. */
  private val PatternRangeStep = s"${DiscreteType.name}\\(min=(-?\\d+),max=(-?\\d+),step=(\\d+)\\)".r

  /** Pattern for matching short string discrete schema with step size. */
  private val PatternStep = s"${DiscreteType.name}\\(step=(\\d+)\\)".r
}

/** Trait for schemas for categorical variables. */
trait CategoricalSchema[T] extends Schema[T] {
  /** Values the variable can take. */
  val domain: Either[Set[T], Regex]

  def validate(value: Value[T]): Boolean = domain
    .fold(set => set.isEmpty || set.contains(value.value), regex => regex.pattern.matcher(value.toShortString).matches)
}

/**
 * Schema for nominal variables.
 *
 * @param domain The values of the variable or a regular expression.
 */
case class NominalSchema[T](domain: Either[Set[T], Regex] = Left(Set.empty[T])) extends CategoricalSchema[T] {
  val classification = NominalType

  override protected def paramString(codec: Codec[T]): String = domain
    .fold(set => SchemaParameters.writeSet(set, codec), regex => SchemaParameters.writePattern(regex))
}

/** Companion object to `NominalSchema` case class. */
object NominalSchema {
  /** Pattern for matching short string nominal schema. */
  val Pattern = s"${NominalType.name}.*".r

  /**
   * Construct a nominal schema with a set of values.
   *
   * @param domain The values of the variable.
   */
  def apply[T](domain: Set[T]): NominalSchema[T] = NominalSchema(Left(domain))

  /**
   * Construct a nominal schema with a regular expression for the values.
   *
   * @param domain The regular expression constraining the value.
   */
  def apply[T](domain: Regex): NominalSchema[T] = NominalSchema(Right(domain))

  /**
   * Parse a nominal schema from string components.
   *
   * @param dom   The domain value strings to parse.
   * @param codec The codec to parse with.
   *
   * @return A `Some[NominalSchema]` if successful, `None` otherwise.
   */
  def fromComponents[T](dom: Set[String], codec: Codec[T]): Option[NominalSchema[T]] = {
    val values = dom.flatMap(SchemaParameters.parse(codec, _))

    if (values.isEmpty || values.size != dom.size) None else Option(NominalSchema(Left(values)))
  }

  /**
   * Parse a nominal schema from string.
   *
   * @param str   The string to parse.
   * @param codec The codec to parse with.
   *
   * @return A `Some[NominalSchema]` if successful, `None` otherwise.
   */
  def fromShortString[T](str: String, codec: Codec[T]): Option[NominalSchema[T]] = str match {
    case PatternName() => Option(NominalSchema())
    case PatternEmpty() => Option(NominalSchema())
    case PatternRegex(pattern) => Option(NominalSchema(pattern.r))
    case PatternSet(set) => fromComponents(SchemaParameters.splitSet(set), codec)
    case _ => None
  }

  /** Pattern for matching short string nominal schema without arguments. */
  private val PatternEmpty = s"${NominalType.name}\\(\\)".r

  /** Pattern for matching short string nominal schema. */
  private val PatternName = NominalType.name.r

  /** Pattern for matching short string nominal schema with a pattern. */
  private val PatternRegex = s"${NominalType.name}\\(pattern=(.*)\\)".r

  /** Pattern for matching short string nominal schema with a set of values. */
  private val PatternSet = s"${NominalType.name}\\(set=\\{(.*)\\}\\)".r
}

/**
 * Schema for ordinal variables.
 *
 * @param domain The optional values of the variable or a regular expression.
 */
case class OrdinalSchema[
  T : Ordering
](
  domain: Either[Set[T], Regex] = Left(Set.empty[T])
) extends CategoricalSchema[T] {
  val classification = OrdinalType

  override protected def paramString(codec: Codec[T]): String = domain
    .fold(set => SchemaParameters.writeOrderedSet(set, codec), regex => SchemaParameters.writePattern(regex))
}

/** Companion object to `OrdinalSchema`. */
object OrdinalSchema {
  /** Pattern for matching short string ordinal schema. */
  val Pattern = s"${OrdinalType.name}.*".r

  /**
   * Construct an ordinal schema with a set of values.
   *
   * @param domain The values of the variable.
   */
  def apply[T : Ordering](domain: Set[T]): OrdinalSchema[T] = OrdinalSchema(Left(domain))

  /**
   * Construct an ordinal schema with a regular expression for the values.
   *
   * @param domain The regular expression constraining the value.
   */
  def apply[T : Ordering](domain: Regex): OrdinalSchema[T] = OrdinalSchema(Right(domain))

  /**
   * Parse a ordinal schema from string components.
   *
   * @param dom   The domain value strings to parse.
   * @param codec The codec to parse with.
   *
   * @return A `Some[OrdinalSchema]` if successful, `None` otherwise.
   */
  def fromComponents[T](dom: Set[String], codec: Codec[T]): Option[OrdinalSchema[T]] = {
    val values = dom.flatMap(SchemaParameters.parse(codec, _))

    if (values.isEmpty || values.size != dom.size) None else Option(OrdinalSchema(Left(values))(codec.ordering))
  }

  /**
   * Parse a ordinal schema from string.
   *
   * @param str   The string to parse.
   * @param codec The codec to parse with.
   *
   * @return A `Some[OrdinalSchema]` if successful, `None` otherwise.
   */
  def fromShortString[T](str: String, codec: Codec[T]): Option[OrdinalSchema[T]] = str match {
    case PatternEmpty() => Option(OrdinalSchema()(codec.ordering))
    case PatternName() => Option(OrdinalSchema()(codec.ordering))
    case PatternRegex(pattern) => Option(OrdinalSchema(pattern.r)(codec.ordering))
    case PatternSet(set) => fromComponents(SchemaParameters.splitSet(set), codec)
    case _ => None
  }

  /** Pattern for matching short string ordinal schema without arguments. */
  private val PatternEmpty = s"${OrdinalType.name}\\(\\)".r

  /** Pattern for matching short string ordinal schema. */
  private val PatternName = OrdinalType.name.r

  /** Pattern for matching short string ordinal schema with a pattern. */
  private val PatternRegex = s"${OrdinalType.name}\\(pattern=(.*)\\)".r

  /** Pattern for matching short string ordinal schema with a set of values. */
  private val PatternSet = s"${OrdinalType.name}\\(set=\\{(.*)\\}\\)".r
}

/**
 * Schema for pair variables.
 *
 * TODO: finish this
 */
case class PairSchema[X, Y](xSchema: Schema[X], ySchema: Schema[Y]) extends Schema[(X, Y)] {
  val classification = PairType(xSchema.classification, ySchema.classification)

  def validate(value: Value[(X, Y)]): Boolean = true
}

object PairSchema {
  val Pattern = s"${PairType.name}.*".r
  /**
    * Parse a pair schema from string.
    *
    * @param str   The string to parse.
    * @param codec The codec to parse with.
    *
    * @return A `Some[PairSchema[X, Y]]` if successful, `None` otherwise.
    */
  def fromShortString[
    X,
    Y
  ](
    str: String,
    xSchema: Schema[X],
    ySchema: Schema[Y],
    codec: PairCodec[X, Y]
  ): Option[PairSchema[X, Y]] = str match {
    case PatternPair(x, y) if x == xSchema.toShortString(codec.xCodec) && y == ySchema.toShortString(codec.yCodec) =>
      Option(PairSchema[X, Y](xSchema, ySchema))
    case _ => None
  }

  /** Pattern for matching short string nominal schema. */
  private val PatternPair = s"${PairType.name}\\((.+),(.+)\\)".r
}

/**
 * Schema for date variables.
 *
 * @param dates The values of the variable, a set or range.
 */
case class DateSchema[T <% Date](dates: Either[Set[T], (Option[T], Option[T])] = Left(Set.empty[T])) extends Schema[T] {
  val classification = DateType

  def validate(value: Value[T]): Boolean = dates.fold(
    domain => domain.isEmpty || domain.contains(value.value),
    range => range._1.fold(true)(t => value.cmp(t) >= 0) && range._2.fold(true)(t => value.cmp(t) <= 0)
  )

  override protected def paramString(codec: Codec[T]): String = dates.fold(
    domain => SchemaParameters.writeSet(domain, codec),
    range => SchemaParameters.writeRange(range._1, range._2, codec, List.empty)
  )
}

/** Companion object to `DateSchema`. */
object DateSchema {
  /** Pattern for matching short string date schema. */
  val Pattern = s"${DateType.name}.*".r

  /**
   * Construct a date schema with bounded range.
   *
   * @param min The minimum value.
   * @param max The maximum value.
   */
  def apply[T <% Date](min: T, max: T): DateSchema[T] = DateSchema(Right((Option(min), Option(max))))

  /**
   * Construct a date schema with a set of valid dates.
   *
   * @param domain The set of legal values.
   */
  def apply[T <% Date](domain: Set[T]): DateSchema[T] = DateSchema(Left(domain))

  /**
   * Parse a date schema from components.
   *
   * @param min   The minimum value string to parse.
   * @param max   The maximum value string to parse.
   * @param codec The codec to parse with.
   *
   * @return A `Some[DateSchema]` if successful, `None` otherwise.
   */
  def fromComponents[T](min: String, max: String, codec: Codec[T]): Option[DateSchema[T]] = for {
    low <- SchemaParameters.parseWrapped(codec, min)
    upp <- SchemaParameters.parseWrapped(codec, max)
    ev <- codec.date
  } yield DateSchema(Right((low, upp)))(ev)

  /**
   * Parse a date schema from string components.
   *
   * @param dom   The domain value strings to parse.
   * @param codec The codec to parse with.
   *
   * @return A `Some[DateSchema]` if successful, `None` otherwise.
   */
  def fromComponents[T](dom: Set[String], codec: Codec[T]): Option[DateSchema[T]] = {
    val values = dom.flatMap(SchemaParameters.parse(codec, _))

    (codec.date, values.isEmpty || values.size != dom.size) match {
      case (Some(ev), false) => Option(DateSchema(values)(ev))
      case _ => None
    }
  }

  /**
   * Parse a date schema from string.
   *
   * @param str   The string to parse.
   * @param codec The codec to parse with.
   *
   * @return A `Some[DateSchema]` if successful, `None` otherwise.
   */
  def fromShortString[T](str: String, codec: Codec[T]): Option[DateSchema[T]] = str match {
    case PatternEmpty() => codec.date.map(ev => DateSchema()(ev))
    case PatternRange(min, max) => fromComponents(min, max, codec) // must be before min/max due to greedy matching
    case PatternMax(max) => fromComponents("", max, codec)
    case PatternMin(min) => fromComponents(min, "", codec)
    case PatternName() => codec.date.map(ev => DateSchema()(ev))
    case PatternSet(set) => fromComponents(SchemaParameters.splitSet(set), codec)
    case _ => None
  }

  /** Pattern for matching short string date schema without arguments. */
  private val PatternEmpty = s"${DateType.name}\\(\\)".r

  /** Pattern for matching short string date schema with maximum value. */
  private val PatternMax = s"${DateType.name}\\(max=(.*)\\)".r

  /** Pattern for matching short string date schema with minimum value. */
  private val PatternMin = s"${DateType.name}\\(min=(.*)\\)".r

  /** Pattern for matching short string date schema. */
  private val PatternName = DateType.name.r

  /** Pattern for matching short string date schema with range. */
  private val PatternRange = s"${DateType.name}\\(min=(.*),max=(.*)\\)".r

  /** Pattern for matching short string date schema with a set of values. */
  private val PatternSet = s"${DateType.name}\\(set=\\{(.*)\\}\\)".r
}

/** Functions for dealing with schema parameters. */
private object SchemaParameters {
  def parse[T](codec: Codec[T], value: String): Option[T] = codec.decode(value)

  def parseWrapped[T](codec: Codec[T], value: String): Option[Option[T]] = {
    if (value.isEmpty) Option(None) else parse(codec, value).map(Option(_))
  }

  def splitSet(
    str: String
  ): Set[String] = if (str.isEmpty) Set.empty else str.split("(?<!\\\\),", -1).map(_.replaceAll("\\\\,", ",")).toSet

  def writeOrderedSet[T : Ordering](set: Set[T], codec: Codec[T]): String = writeList(set.toList.sorted, codec)

  def writeRange[T](min: Option[T], max: Option[T], codec: Codec[T], extra: List[(String, Option[String])]): String = {
    val list = List(
      min.map(t => s"min=${codec.encode(t)}"),
      max.map(t => s"max=${codec.encode(t)}")
    ) ++ extra.map { case (name, value) => value.map(v => s"${name}=${v}") }

    list.flatten.mkString(",")
  }

  def writePattern(regex: Regex): String = s"pattern=${regex.toString}"

  def writeSet[T](set: Set[T], codec: Codec[T]): String = writeList(set.toList, codec)

  private def writeList[T](list: List[T], codec: Codec[T]): String = {
    if (list.isEmpty) "" else list.map(d => codec.encode(d).replaceAll(",", "\\\\,")).mkString("set={", ",", "}")
  }
}

