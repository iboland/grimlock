// Copyright 2016,2017,2018,2019 Commonwealth Bank of Australia
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

package commbank.grimlock.spark.environment

import commbank.grimlock.framework.{ ParquetConfig, Persist }
import commbank.grimlock.framework.environment.{ Context => FwContext }

import org.apache.hadoop.io.Writable

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ Encoder, Row, SparkSession }

import scala.reflect.ClassTag
import scala.util.{ Failure, Success }

/**
 * Spark operating context state.
 *
 * @param session The Spark session.
 */
case class Context(session: SparkSession) extends FwContext[Context] {
  type E[T] = Context.E[T]

  type U[T] = Context.U[T]

  def loadText[
    T : ClassTag
  ](
    file: String,
    parser: Persist.TextParser[T]
  ): (Context.U[T], Context.U[Throwable]) = {
    val rdd = session.sparkContext.textFile(file).flatMap { case s => parser(s) }

    (rdd.collect { case Success(c) => c }, rdd.collect { case Failure(e) => e })
  }

  def loadSequence[
    K <: Writable : Manifest,
    V <: Writable : Manifest,
    T : ClassTag
  ](
    file: String,
    parser: Persist.SequenceParser[K, V, T]
  ): (Context.U[T], Context.U[Throwable]) = {
    val rdd = session.sparkContext.sequenceFile[K, V](file).flatMap { case (k, v) => parser(k, v) }

    (rdd.collect { case Success(c) => c }, rdd.collect { case Failure(e) => e })
  }

  def loadParquet[
    X,
    T : ClassTag
  ](
    file: String,
    parser: Persist.ParquetParser[X, T]
  )(implicit
    cfg: ParquetConfig[X, Context]
  ): (Context.U[T], Context.U[Throwable]) = {
    val rdd = cfg.load(this, file).flatMap(v => parser(v))

    (rdd.collect { case Success(c) => c }, rdd.collect { case Failure(e) => e })
  }

  val implicits = Implicits

  val library = Library

  def empty[T : ClassTag]: Context.U[T] = session.sparkContext.parallelize(List.empty[T])

  def from[T : ClassTag](seq: Seq[T]): Context.U[T] = session.sparkContext.parallelize(seq)

  def nop(): Unit = ()
}

/** Companion object to `Context` with additional constructors and implicits. */
object Context {
  /** Type for user defined data. */
  type E[T] = T

  /** Type for distributed data. */
  type U[T] = RDD[T]

  /**
   * Implicit function that provides spark parquet reader implementation. The method uses
   * `DataFrameReader` to read parquet.
   */
  implicit val toSparkRowParquet = new ParquetConfig[Row, Context] {
    def load(context: Context, file: String): Context.U[Row] = context.session.sqlContext.read.parquet(file).rdd
  }

  /**
   * Implicit function that provides spark parquet reader implementation. The method uses
   * `DataFrameReader` to read parquet.
   */
  implicit def toSparkParquet[T : ClassTag](implicit ev: Encoder[T]) = new ParquetConfig[T, Context] {
    def load(
      context: Context,
      file: String
    ): Context.U[T] = context.session.sqlContext.read.parquet(file).as[T].rdd
  }
}

