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

package commbank.grimlock.framework.pair

import commbank.grimlock.framework.Cell
import commbank.grimlock.framework.aggregate.{ Aggregator => Agg, Single }
import commbank.grimlock.framework.content.Content
import commbank.grimlock.framework.encoding.{ Codec, Value }
import commbank.grimlock.framework.metadata.{ Schema, Type }
import commbank.grimlock.framework.position.Position

import java.util.Date

import scala.reflect.{ classTag, ClassTag }
import scala.reflect.runtime.universe.{ typeTag, TypeTag }
import scala.util.matching.Regex

import shapeless.{ HList, Nat }

/** Codec for dealing with a pair of values. Each value must have a corresponding `Codec`. */
case class PairCodec[
  X : TypeTag,
  Y : TypeTag
](
  xCodec: Codec[X],
  yCodec: Codec[Y],
  open: Char = '(',
  separator: Char = ',',
  close: Char = ')'
 ) extends Codec[(X, Y)] { self =>
  val converters: Set[Codec.Convert[(X, Y)]] = Set.empty
  val date: Option[((X, Y)) => Date] = None
  val integral: Option[Integral[(X, Y)]] = None
  val numeric: Option[Numeric[(X, Y)]] = None
  def ordering: Ordering[(X, Y)] = new Ordering[(X, Y)] {
    def compare(x: (X, Y), y: (X, Y)): Int = self.compare(x, y)
  }

  def box(value: (X, Y)): Value[(X, Y)] = PairValue(value, this)

  def compare(
    x: (X, Y),
    y: (X, Y)
  ): Int = if (xCodec.compare(x._1, y._1) == 0) yCodec.compare(x._2, y._2) else xCodec.compare(x._1, y._1)

  def decode(
    str: String
  ): Option[(X, Y)] = {
    val pattern = s"\\${open}(.+)\\${separator}(.+)\\${close}".r
    val (x, y) = str match {
      case pattern(left, right) => (xCodec.decode(left), yCodec.decode(right))
      case _ => (None, None)
    }
    for (a <- x; b <- y) yield (a, b)
  }

  def encode(value: (X, Y)): String = s"${open}${xCodec.encode(value._1)}${separator}${yCodec.encode(value._2)}${close}"

  def toShortString = s"pair${separator.toString}${xCodec.toShortString}${separator.toString}${yCodec.toShortString}${separator.toString}${open.toString}${separator.toString}${close.toString}"
}

object PairCodec {
  /** Pattern for parsing `PairCodec` from string with xCodec and yCodec provided. */
  val Pattern: Regex = raw"pair(.)(.+)\1(.+)\1(.)\1(.)".r

  /**
   * Parse a PairCodec[X, Y] from a string.
   *
   * @param str String from which to parse the codec.
   * @param xCodec Left Codec[X] to attempt to parse PairCodec.
   * @param yCodec Left Codec[Y] to attempt to parse PairCodec.
   *
   * @return A `Some[PairCodec[X, Y]]` in case of success, `None` otherwise.
   */
  def fromShortString[X : TypeTag, Y : TypeTag](str: String, xCodec: Codec[X], yCodec: Codec[Y]): Option[PairCodec[X, Y]] = str match {
    case Pattern(sep, x, y, open, close) if xCodec.toShortString == x && yCodec.toShortString == y =>
      // sep, open, close are regex-ed to be single length strings. So we can safely take their first element
      Option(PairCodec(xCodec, yCodec, open.head, sep.head, close.head))
    case _ => None
  }
}

case class PairValue[
  X: TypeTag,
  Y: TypeTag
](
  value: (X, Y),
  codec: PairCodec[X, Y]
) extends Value[(X, Y)] {
  protected val ttag: TypeTag[(X, Y)] = typeTag[(X, Y)]

  def cmp[V <% Value[_]](that: V): Option[Int] = that.as[(X, Y)].map(t => cmp(t))
}

/** Companion object to `PairValue` case class. */
object PairValue {
  /** `unapply` method for pattern matching. */
  def unapply[X, Y](value: Value[_]): Option[(X, Y)] = classTag[(X, Y)].unapply(value.value)
}

case object PairType extends Type {
  val name = "pair"

  def toShortString: String = name.toString
}

case class PairSchema[X, Y]() extends Schema[(X, Y)] {
  val classification = PairType

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
   * @return A `Some[PairSchema]` if successful, `None` otherwise.
   */
  def fromShortString[X, Y](str: String, codec: PairCodec[X, Y]): Option[PairSchema[X, Y]] = str match {
    case PatternName() => Option(PairSchema[X, Y]())
    case _ => None
  }

  /** Pattern for matching short string nominal schema. */
  private val PatternName = PairType.name.r
}

/* Aggregator to create PairValues
*
* Given two concatenated C#U[Cell[P]] values: `left` and `right`, with unique values within them but
* have matching coordinates between them, it will produce a single C#U with all matching coordinates
* filled to have `PairValue`s. This operation is conceptually similar to an inner join, with the
* difference being that if duplicates are found all rows for that entry are discarded.
*
* @param left         A `PairSpec` detailing the settings desired for the left data.
* @param right        A `PairSpec` detailing the settings desired for the right data.
* @param dim          The dimension as a shapeless `Nat` for which the key string is to be found.
* */
case class GeneratePair[
  P <: HList,
  S <: HList,
  D <: Nat,
  X : ClassTag : TypeTag,
  Y : ClassTag : TypeTag
](
  left: PairSpec[X],
  right: PairSpec[Y],
  dim: D
)(implicit
  ev1: Position.IndexConstraints.Aux[P, D, Value[String]]
) extends Agg[P, S, S] {
  type T = List[(String, Either[X, Y])]
  type O[A] = Single[A]

  val tTag = classTag[T]
  val oTag = classTag[O[_]]

  def prepare(cell: Cell[P]): Option[T] = cell.position(dim).value match {
    case str: String if str == left.id => cell.content.value.as[X].map(d => List((str, Left(d))))
    case str: String if str == right.id => cell.content.value.as[Y].map(d => List((str, Right(d))))
    case _ => None
  }

  def reduce(lt: T, rt: T): T = lt ++ rt

  def present(pos: Position[S], t: T): O[Cell[S]] = t match {
    case List((str1, Left(x)), (str2, Right(y))) => createSingle(pos, x, y)
    case List((str1, Right(y)), (str2, Left(x))) => createSingle(pos, x, y)
    case List((str, Left(x))) =>
      right.default.map(createSingle(pos, x, _)).getOrElse(Single())
    case List((str, Right(y))) =>
      left.default.map(createSingle(pos, _, y)).getOrElse(Single())
    case _ => Single()
  }

  private def createSingle(position: Position[S], l: X, r: Y): O[Cell[S]] = {
    Single(
      Cell(
        position,
        Content(
          PairSchema[X, Y](),
          PairValue((l, r), PairCodec(left.codec, right.codec))
        )
      )
    )
  }
}

/** Configuration class for one side of the `GeneratePair` aggregator.
  *
  * @param id      String indicating the key that identifies this Spec.
  * @param codec   A codec of type X for which the data should attempt to be cast.
  * @param default Optionally, a default value for the data.
  */
case class PairSpec[X](id: String, codec: Codec[X], default: Option[X] = None)
