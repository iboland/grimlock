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

import shapeless.{ :+:, CNil, Coproduct }
import shapeless.ops.coproduct.Inject

/** Trait for variable types. */
trait Type extends java.io.Serializable {
  /** Returns the most general super type of `this`. */
  def getRootType: Type = parent.map(_.getRootType).getOrElse(this)

  /** Check if this is a sub-type of `that`. */
  def isOfType(that: Type): Boolean = (this == that) || parent.map(_.isOfType(that)).getOrElse(false)

  /** Return the first type in common between this and `that`, or `Mixed` in case of no common ancestor. */
  def getCommonType(that: Type): Type = {
    if (this == that) this
    else if (this.isRootType) { if (that.isOfType(this)) this else MixedType }
    else if (that.isRootType) { if (this.isOfType(that)) that else MixedType }
    else sharedParent(that).getOrElse(MixedType)
  }

  override def toString: String = toShortString.capitalize + "Type"

  /** Return a consise (terse) string representation of a type. */
  def toShortString: String

  protected val parent: Option[Type] = None

  private def isRootType: Boolean = parent.isEmpty
  private def sharedParent(that: Type): Option[Type] = parent
    .flatMap(p => if (that.isOfType(p)) Option(p) else p.sharedParent(that))
}

/** Companion object to `Type` trait. */
object Type {
  /** Type for a default Type co-product when parsing Types from string. */
  type DefaultTypes = CategoricalType.type :+:
    ContinuousType.type :+:
    DateType.type :+:
    DiscreteType.type :+:
    MixedType.type :+:
    NominalType.type :+:
    NumericType.type :+:
    OrdinalType.type :+:
    CNil

  /** Type that captures all constraints for parsing default Types from string. */
  trait TextParseConstraints[C <: Coproduct] extends java.io.Serializable {
    implicit val asCategorical: Inject[C, CategoricalType.type]
    implicit val asContinuous: Inject[C, ContinuousType.type]
    implicit val asDate: Inject[C, DateType.type]
    implicit val asDiscrete: Inject[C, DiscreteType.type]
    implicit val asMixed: Inject[C, MixedType.type]
    implicit val asNominal: Inject[C, NominalType.type]
    implicit val asNumeric: Inject[C, NumericType.type]
    implicit val asOrdinal: Inject[C, OrdinalType.type]
  }

  /** Implicit meeting text parsing constraints for the default Types. */
  implicit def typeTextParseConstraints[
    C <: Coproduct
  ](implicit
    ev1: Inject[C, CategoricalType.type],
    ev2: Inject[C, ContinuousType.type],
    ev3: Inject[C, DateType.type],
    ev4: Inject[C, DiscreteType.type],
    ev5: Inject[C, MixedType.type],
    ev6: Inject[C, NominalType.type],
    ev7: Inject[C, NumericType.type],
    ev8: Inject[C, OrdinalType.type]
  ): TextParseConstraints[C] = new TextParseConstraints[C] {
    implicit val asCategorical = ev1
    implicit val asContinuous = ev2
    implicit val asDate = ev3
    implicit val asDiscrete = ev4
    implicit val asMixed = ev5
    implicit val asNominal = ev6
    implicit val asNumeric = ev7
    implicit val asOrdinal = ev8
  }

  /**
   * Parse a type from a string.
   *
   * @param str String from which to parse the type.
   *
   * @return A `Some[C]` in case of success, `None` otherwise.
   */
  def fromShortString[C <: Coproduct](str: String)(implicit ev: TextParseConstraints[C]): Option[C] = {
    import ev._

    str match {
      case CategoricalType.name => Option(Coproduct(CategoricalType))
      case ContinuousType.name => Option(Coproduct(ContinuousType))
      case DateType.name => Option(Coproduct(DateType))
      case DiscreteType.name => Option(Coproduct(DiscreteType))
      case MixedType.name => Option(Coproduct(MixedType))
      case NominalType.name => Option(Coproduct(NominalType))
      case NumericType.name => Option(Coproduct(NumericType))
      case OrdinalType.name => Option(Coproduct(OrdinalType))
      case _ => None
    }
  }
}

/** Type for when the type is mixed. */
case object MixedType extends Type {
  /** Short name for this type. */
  val name = "mixed"

  def toShortString: String = MixedType.name
}

/** Type for numeric types. */
trait NumericType extends Type {
  def toShortString: String = NumericType.name
}

/** Companion object to `NumericType` trait. */
case object NumericType extends NumericType {
  /** Short name for this type. */
  val name = "numeric"
}

/** Type for continuous types. */
case object ContinuousType extends NumericType {
  /** Short name for this type. */
  val name = "continuous"

  override def toShortString: String = ContinuousType.name

  override protected val parent: Option[Type] = Some(NumericType)
}

/** Type for discrete types. */
case object DiscreteType extends NumericType {
  /** Short name for this type. */
  val name = "discrete"

  override def toShortString: String = DiscreteType.name

  override protected val parent: Option[Type] = Some(NumericType)
}

/** Type for categorical types. */
trait CategoricalType extends Type {
  def toShortString: String = CategoricalType.name
}

/** Companion object to `CategoricalType` trait. */
case object CategoricalType extends CategoricalType {
  /** Short name for this type. */
  val name = "categorical"
}

/** Type for nominal types. */
case object NominalType extends CategoricalType {
  /** Short name for this type. */
  val name = "nominal"

  override def toShortString: String = NominalType.name

  override protected val parent: Option[Type] = Some(CategoricalType)
}

/** Type for ordinal types. */
case object OrdinalType extends CategoricalType {
  /** Short name for this type. */
  val name = "ordinal"

  override def toShortString: String = OrdinalType.name

  override protected val parent: Option[Type] = Some(CategoricalType)
}

/** Type for date types. */
case object DateType extends Type {
  /** Short name for this type. */
  val name = "date"

  def toShortString: String = DateType.name
}

/** Type for pair types. */
case object PairType extends Type {
  /** Short name for this type. */
  val name = "pair"

  def toShortString: String = PairType.name
}
