// Copyright 2014,2015,2016 Commonwealth Bank of Australia
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

package au.com.cba.omnia.grimlock.spark.examples

import au.com.cba.omnia.grimlock.framework._
import au.com.cba.omnia.grimlock.framework.content._
import au.com.cba.omnia.grimlock.framework.content.metadata._
import au.com.cba.omnia.grimlock.framework.encoding._
import au.com.cba.omnia.grimlock.framework.partition._
import au.com.cba.omnia.grimlock.framework.position._

import au.com.cba.omnia.grimlock.library.aggregate._

import au.com.cba.omnia.grimlock.spark.environment._
import au.com.cba.omnia.grimlock.spark.Matrix._
import au.com.cba.omnia.grimlock.spark.partition.Partitions._
import au.com.cba.omnia.grimlock.spark.position.PositionDistributable._

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd.RDD

// Assign cell to 1 or more partitions based on the hash-code in the 3rd dimension.
case class EnsembleSplit(gbm: String, rf: String, lr: String) extends Partitioner[Position3D, String] {
  // Depending on the hash-code assign the cell to the appropriate partition:
  //   [0, 4) -> gbm
  //   [2, 6) -> rf
  //   [4, 8) -> lr
  //   [8, 9] -> all
  def assign(cell: Cell[Position3D]): TraversableOnce[String] = {
    cell
      .position(Third)
      .asLong
      .map {
        case hash =>
          if (hash < 2) List(gbm)
          else if (hash < 4) List(gbm, rf)
          else if (hash < 6) List(rf, lr)
          else if (hash < 8) List(lr)
          else List(gbm, rf, lr)
      }
      .getOrElse(List())
  }
}

// Simple ensemble(-like) model learning
object Ensemble {

  def main(args: Array[String]) {
    // Define implicit context.
    implicit val ctx = Context(new SparkContext(args(0), "Grimlock Spark Demo", new SparkConf()))

    // Path to data files, output folder
    val path = if (args.length > 1) args(1) else "../../data"
    val output = "spark"

    // Define table schema (column name and variable type tuple).
    val schema = List(("entity.id", Content.parser(StringCodec, NominalSchema[String]())),
      ("label", Content.parser(LongCodec, NominalSchema[Long]())),
      ("x", Content.parser(DoubleCodec, ContinuousSchema[Double]())),
      ("y", Content.parser(DoubleCodec, ContinuousSchema[Double]())),
      ("z", Content.parser(DoubleCodec, ContinuousSchema[Double]())))

    // Load the data (generated by ensemble.R); returns a 2D matrix (instance x feature);
    val (data, _) = loadText(s"${path}/exampleEnsemble.txt", Cell.parseTable(schema, separator = "|"))

    // Ensemble scripts to apply to the data.
    val scripts = List("gbm.R", "lr.R", "rf.R")

    // Weigh each model equally.
    val weights = Map(Position1D(scripts(0)) -> Content(ContinuousSchema[Double](), 0.33),
      Position1D(scripts(1)) -> Content(ContinuousSchema[Double](), 0.33),
      Position1D(scripts(2)) -> Content(ContinuousSchema[Double](), 0.33))

    // Type of the weights map.
    type W = Map[Position1D, Content]

    // Train and score the data.
    //
    // The partition key is the script to apply to the model. This approach assumes that the scripts split
    // the data into a ~40% train set per script and a shared ~20% test set. The returned scores are for
    // the test set. The resulting scores are expanded with the model name so they can be merged (see below).
    def trainAndScore(key: String, partition: RDD[Cell[Position3D]]): RDD[Cell[Position2D]] = {
      partition
        .stream("Rscript " + key, List(key), Cell.toString("|", false, true), Cell.parse1D("|", StringCodec))
        .data // Keep only the data (ignoring errors).
        .relocate(Locate.AppendValue[Position1D](key))
    }

    // Define extractor to get weight out of weights map.
    val extractWeight = ExtractWithDimension[Position2D, Content](Second).andThenPresent(_.value.asDouble)

    // Train and score an ensemble:
    // 1/ Expand with a dimension that holds the hash-code base 10 (for use in partitioning);
    // 2/ Partition the data roughly into 40% for each model, plus shared 20% for scoring;
    // 3/ For each parition, apply the 'train and score' function;
    // 4/ Merge the scores of each model into a single 2D matrix (instance x model);
    // 5/ Apply a weighted sum to the model scores (this can be leared by streaming the scores);
    // 6/ Persist the final scores.
    // 7/ Compact the scores into a Map so they can be used to compute the Gini index with.
    val scores = data
      .relocate(c => c.position.append(math.abs(c.position(First).hashCode % 10)).toOption)
      .split(EnsembleSplit(scripts(0), scripts(1), scripts(2)))
      .forEach(scripts, trainAndScore)
      .merge(scripts)
      .summariseWithValue(Over(First), WeightedSum[Position2D, Position1D, W](extractWeight), weights)
      .saveAsText(s"./demo.${output}/ensemble.scores.out")
      .compact(Over(First))

    // Rename instance id (first dimension) with its score
    def renameWithScore(cell: Cell[Position2D], ext: Map[Position1D, Content]): Option[Position2D] = {
      ext.get(Position1D(cell.position(First))).map { case con => Position2D(con.value, cell.position(Second)) }
    }

    // Compute Gini Index on ensemble scores:
    // 1/ Keep only 'label' column, results in 2D matrix (instance x label) with 1 column;
    // 2/ Filter rows to only keep instances for which a score is available and rename instance id with score;
    // 3/ Compute Gini Index (this sorts the labels by score as its a dimension);
    // 4/ Persist the Gini Index to file.
    data
      .slice(Over(Second), "label", true)
      .relocateWithValue(renameWithScore, scores)
      .gini(Over(Second))
      .saveAsText(s"./demo.${output}/ensemble.gini.out")
      .toUnit
  }
}

