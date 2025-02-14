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

package commbank.grimlock.framework.squash

import commbank.grimlock.framework.Cell
import commbank.grimlock.framework.content.Content
import commbank.grimlock.framework.encoding.Value
import commbank.grimlock.framework.position.Position

import scala.reflect.ClassTag

import shapeless.{ HList, Nat }

/** Trait for squashing a dimension. */
trait Squasher[P <: HList] extends SquasherWithValue[P] {
  type V = Any

  def prepareWithValue[
    D <: Nat
  ](
    cell: Cell[P],
    dim: D,
    ext: V
  )(implicit
    ev: Position.IndexConstraints[P, D] { type V <: Value[_] }
  ): Option[T] = prepare(cell, dim)

  def presentWithValue(t: T, ext: V): Option[Content] = present(t)

  /**
   * Prepare for squashing.
   *
   * @param cell Cell which is to be squashed.
   * @param dim  The dimension being squashed.
   *
   * @return State to reduce.
   */
  def prepare[
    D <: Nat
  ](
    cell: Cell[P],
    dim: D
  )(implicit
    ev: Position.IndexConstraints[P, D] { type V <: Value[_] }
  ): Option[T]

  /**
   * Present the squashed content.
   *
   * @param t The reduced state.
   *
   * @return The squashed content.
   */
  def present(t: T): Option[Content]
}

/** Trait for squashing a dimension with a user provided value. */
trait SquasherWithValue[P <: HList] extends java.io.Serializable {
  /** Type of the state being squashed. */
  type T

  /** Type of the external value. */
  type V

  /** ClassTag of type of the state being squashed. */
  val tTag: ClassTag[T]

  /**
   * Prepare for squashing.
   *
   * @param cell Cell which is to be squashed.
   * @param dim  The dimension being squashed.
   * @param ext  User provided data required for preparation.
   *
   * @return State to reduce.
   */
  def prepareWithValue[
    D <: Nat
  ](
    cell: Cell[P],
    dim: D,
    ext: V
  )(implicit
    ev: Position.IndexConstraints[P, D] { type V <: Value[_] }
  ): Option[T]

  /**
   * Standard reduce method.
   *
   * @param lt The left state to reduce.
   * @param rt The right state to reduce.
   */
  def reduce(lt: T, rt: T): T

  /**
   * Present the squashed content.
   *
   * @param t   The reduced state.
   * @param ext User provided data required for presentation.
   *
   * @return The squashed content.
   */
  def presentWithValue(t: T, ext: V): Option[Content]
}

