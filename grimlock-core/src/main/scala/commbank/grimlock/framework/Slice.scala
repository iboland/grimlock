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

package commbank.grimlock.framework.position

import commbank.grimlock.framework.encoding.Value

import shapeless.{ ::, HList, HNil, Nat, Witness }

/** Trait that encapsulates dimension on which to operate. */
trait Slice[P <: HList, S <: HList, R <: HList] {
  /** Returns the selected coordinate(s) for the given `pos`. */
  def selected(pos: Position[P]): Position[S]

  /** Returns the remaining coordinate(s) for the given `pos`. */
  def remainder(pos: Position[P]): Position[R]
}

/**
 * Indicates that the selected coordinates are indexed by `dimensions`. In other words, when a groupBy is performed,
 * it is performed using a `Position` consisting of the coordinates at the indices given by `dimensions`. If more than
 * one dimension is provided then these dimensions must be supplied in ascending order of their indices and cannot be
 * repeated.
 *
 * @param dimensions Dimensions of the selected coordinates.
 */
case class Over[
  P <: HList,
  I <: HList,
  S <: HList,
  R <: HList
](
  dimensions: I
)(implicit
  ev1: Position.IndexConstraints.Aux[P, I, S],
  ev2: Position.RemoveConstraints.Aux[P, I, R]
) extends Slice[P, S, R] {
  def selected(pos: Position[P]): Position[S] = Position(pos(dimensions))

  def remainder(pos: Position[P]): Position[R] = pos.remove(dimensions)
}

/** Companion object to `Over` case class. */
object Over {
  /**
   * Construct an `Over` for 1 dimension.
   *
   * @param dimension Dimension of the selected coordinate.
   */
  def apply[
    P <: HList,
    D <: Nat,
    V <: Value[_],
    R <: HList
  ](
    dimension: D
  )(implicit
    // different parameter order to case class needed to prevent duplicate symbols compile error
    ev1: Position.RemoveConstraints.Aux[P, D, R],
    ev2: Position.IndexConstraints.Aux[P, D, V]
  ): Over[P, D :: HNil, V :: HNil, R] = Over(dimension :: HNil)

  /**
   * Construct an `Over` for 2 dimensions.
   *
   * @param first First dimension to select.
   * @param second Second dimension to select.
   */
  def apply[
    P <: HList,
    D <: Nat,
    E <: Nat,
    S <: HList,
    R <: HList
  ](
    first: D,
    second: E
  )(implicit
    ev1: Position.IndexConstraints.Aux[P, D :: E :: HNil, S],
    ev2: Position.RemoveConstraints.Aux[P, D :: E :: HNil, R]
  ): Over[P, D :: E :: HNil, S, R] = Over(first :: second :: HNil)

  /** Construct an `Over` for 1 dimension using types. */
  def apply[
    P <: HList,
    D <: Nat
  ](implicit
    ev1: Position.IndexConstraints[P, D :: HNil] { type V <: HList },
    ev2: Position.RemoveConstraints[P, D :: HNil],
    ev3: Witness.Aux[D]
  ): Over[P, D :: HNil, ev1.V, ev2.Q] =
    // implicits must be passed explicitly to case class constructor so the compiler knows which constructor is
    // being called. Same behaviour for the other apply methods which use types
    Over(ev3.value :: HNil)(ev1, ev2)

  /** Construct an `Over` for 2 dimensions using types. */
  def apply[
    P <: HList,
    D <: Nat,
    E <: Nat
  ](implicit
    ev1: Position.IndexConstraints[P, D :: E :: HNil] { type V <: HList },
    ev2: Position.RemoveConstraints[P, D :: E :: HNil],
    ev3: Witness.Aux[D],
    ev4: Witness.Aux[E]
  ): Over[P, D :: E :: HNil, ev1.V, ev2.Q] = Over(ev3.value :: ev4.value :: HNil)(ev1, ev2)
}

/**
 * Indicates that the selected coordinates are all except thosed indexed by `dimensions`. In other words, when a
 * groupBy is performed, it is performed using a `Position` consisting of all coordinates except those at the indices
 * given in `dimensions`. If more than one dimension is provided then these dimensions must be supplied in ascending
 * order of their indices and cannot be repeated.
 *
 * @param dimensions Dimensions of the coordinates to exclude.
 */
case class Along[
  P <: HList,
  I <: HList,
  R <: HList,
  S <: HList
](
  dimensions: I
)(implicit
  ev1: Position.IndexConstraints.Aux[P, I, R],
  ev2: Position.RemoveConstraints.Aux[P, I, S]
) extends Slice[P, S, R] {
  def selected(pos: Position[P]): Position[S] = pos.remove(dimensions)

  def remainder(pos: Position[P]): Position[R] = Position(pos(dimensions))
}

/** Companion object to `Along` case class. */
object Along {
  /**
   * Construct an `Along` for 1 dimension.
   *
   * @param dimension Dimension of the coordinate to exclude.
   */
  def apply[
    P <: HList,
    D <: Nat,
    V <: Value[_],
    S <: HList
  ](
    dimension: D
  )(implicit
    // different parameter order to case class needed to prevent duplicate symbols compile error
    ev1: Position.RemoveConstraints.Aux[P, D, S],
    ev2: Position.IndexConstraints.Aux[P, D, V]
  ): Along[P, D :: HNil, V :: HNil, S] = Along(dimension :: HNil)

  /**
   * Construct an `Along` for 2 dimensions.
   *
   * @param first First dimension to exclude.
   * @param second Second dimension to exclude.
   */
  def apply[
    P <: HList,
    D <: Nat,
    E <: Nat,
    R <: HList,
    S <: HList
  ](
    first: D,
    second: E
  )(implicit
    ev1: Position.IndexConstraints.Aux[P, D :: E :: HNil, R],
    ev2: Position.RemoveConstraints.Aux[P, D :: E :: HNil, S]
  ): Along[P, D :: E :: HNil, R, S] = Along(first :: second :: HNil)

  /** Construct an `Along` for 1 dimension using types. */
  def apply[
    P <: HList,
    D <: Nat
  ](implicit
    ev1: Position.IndexConstraints[P, D :: HNil] { type V <: HList },
    ev2: Position.RemoveConstraints[P, D :: HNil],
    ev3: Witness.Aux[D]
  ): Along[P, D :: HNil, ev1.V, ev2.Q] = Along(ev3.value :: HNil)(ev1, ev2)

  /** Construct an `Along` for 2 dimensions using types. */
  def apply[
    P <: HList,
    D <: Nat,
    E <: Nat
  ](implicit
    ev1: Position.IndexConstraints[P, D :: E :: HNil] { type V <: HList },
    ev2: Position.RemoveConstraints[P, D :: E :: HNil],
    ev3: Witness.Aux[D],
    ev4: Witness.Aux[E]
  ): Along[P, D :: E :: HNil, ev1.V, ev2.Q] = Along(ev3.value :: ev4.value :: HNil)(ev1, ev2)
}

