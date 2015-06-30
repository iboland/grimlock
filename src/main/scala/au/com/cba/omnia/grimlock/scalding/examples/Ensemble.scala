// Copyright 2014-2015 Commonwealth Bank of Australia
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

package au.com.cba.omnia.grimlock.scalding.examples

import au.com.cba.omnia.grimlock.framework._
import au.com.cba.omnia.grimlock.framework.content._
import au.com.cba.omnia.grimlock.framework.content.metadata._
import au.com.cba.omnia.grimlock.framework.encoding._
import au.com.cba.omnia.grimlock.framework.partition._
import au.com.cba.omnia.grimlock.framework.position._
import au.com.cba.omnia.grimlock.framework.sample._
import au.com.cba.omnia.grimlock.framework.utility._

import au.com.cba.omnia.grimlock.library.aggregate._

import au.com.cba.omnia.grimlock.scalding.Matrix._
import au.com.cba.omnia.grimlock.scalding.Nameable._
import au.com.cba.omnia.grimlock.scalding.partition.Partitions._

import com.twitter.scalding.{ Args, Job }
import com.twitter.scalding.typed.{ TypedPipe, ValuePipe }

// Assign cell to 1 or more partitions based on the hash-code in the 3rd dimension.
case class EnsembleSplit(gbm: String, rf: String, lr: String) extends Partitioner[Position3D, String] {
  // Depending on the hash-code assign the cell to the appropriate partition:
  //   [0, 4) -> gbm
  //   [2, 6) -> rf
  //   [4, 8) -> lr
  //   [8, 9] -> all
  def assign(cell: Cell[Position3D]): Collection[String] = {
    val parts = cell.position(Third).asLong.map {
      case hash => if (hash < 2) List(gbm)
        else if (hash < 4) List(gbm, rf)
        else if (hash < 6) List(rf, lr)
        else if (hash < 8) List(lr)
        else List(gbm, rf, lr)
      }
      .getOrElse(List())

    Collection(parts)
  }
}

// Sample/Filter a cell if its instance id exists in the map `ext`.
case class SampleByScore() extends SamplerWithValue[Position2D] {
  type V = Map[Position1D, Content]

  def selectWithValue(cell: Cell[Position2D], ext: V): Boolean = ext.contains(Position1D(cell.position(First)))
}

// Simple ensemble(-like) model learning
class Ensemble(args: Args) extends Job(args) {

  // Path to data files, output folder
  val path = args.getOrElse("path", "../../data")
  val output = "scalding"

  // Define table schema (column name and variable type tuple).
  val schema = List(("entity.id", NominalSchema[Codex.StringCodex]()),
    ("label", NominalSchema[Codex.LongCodex]()),
    ("x", ContinuousSchema[Codex.DoubleCodex]()),
    ("y", ContinuousSchema[Codex.DoubleCodex]()),
    ("z", ContinuousSchema[Codex.DoubleCodex]()))

  // Load the data (generated by ensemble.R); returns a 2D matrix (instance x feature);
  val data = loadTable(s"${path}/exampleEnsemble.txt", schema, separator = "|")

  // Ensemble scripts to apply to the data.
  val scripts = List(s"${path}/gbm.R", s"${path}/lr.R", s"${path}/rf.R")

  // Weigh each model equally.
  val weights = ValuePipe(Map(Position1D(scripts(0)) -> Content(ContinuousSchema[Codex.DoubleCodex](), 0.33),
    Position1D(scripts(1)) -> Content(ContinuousSchema[Codex.DoubleCodex](), 0.33),
    Position1D(scripts(2)) -> Content(ContinuousSchema[Codex.DoubleCodex](), 0.33)))

  // Type of the weights map.
  type W = Map[Position1D, Content]

  // Train and score the data.
  //
  // The partition key is the script to apply to the model. This approach assumes that the scripts split
  // the data into a ~40% train set per script and a shared ~20% test set. The returned scores are for
  // the test set. The resulting scores are expanded with the model name so they can be merged (see below).
  def trainAndScore(key: String, partition: TypedPipe[Cell[Position3D]]): TypedPipe[Cell[Position2D]] = {
    partition
      .stream("Rscript", key, "|", Cell.parse1D("|", StringCodex))
      .expand((cell: Cell[Position1D]) => cell.position.append(key))
  }

  // Define extractor to get weight out of weights map.
  val extractWeight = ExtractWithDimension[Dimension.Second, Position2D, Content](Second)
    .andThenPresent(_.value.asDouble)

  // Train and score an ensemble:
  // 1/ Expand with a dimension that holds the hash-code base 10 (for use in partitioning);
  // 2/ Partition the data roughly into 40% for each model, plus shared 20% for scoring;
  // 3/ For each parition, apply the 'train and score' function;
  // 4/ Merge the scores of each model into a single 2D matrix (instance x model);
  // 5/ Apply a weighted sum to the model scores (this can be leared by streaming the scores);
  // 6/ Persist the final scores.
  // 7/ Collect the scores in a Map so they can be used to compute the Gini index with.
  val scores = data
    .expand((cell: Cell[Position2D]) => cell.position.append(math.abs(cell.position(First).hashCode % 10)))
    .split[String, EnsembleSplit](EnsembleSplit(scripts(0), scripts(1), scripts(2)))
    .forEach(scripts, trainAndScore)
    .merge(scripts)
    .summariseWithValue[Dimension.First, Position1D, WeightedSum[Position2D, Position1D, W], W](Over(First),
      WeightedSum(extractWeight), weights)
    .save(s"./demo.${output}/ensemble.scores.out")
    .toMap(Over(First))

  // Rename instance id (first dimension) with its score
  def renameWithScore(cell: Cell[Position2D], ext: Map[Position1D, Content]): Position2D = {
    Position2D(ext(Position1D(cell.position(First))).value, cell.position(Second))
  }

  // Compute Gini Index on ensemble scores:
  // 1/ Keep only 'label' column, results in 2D matrix (instance x label) with 1 column;
  // 2/ Filter rows to only keep instances for which a score is available;
  // 3/ Rename instance id with score;
  // 4/ Compute Gini Index (this sorts the labels by score as its a dimension);
  // 5/ Persist the Gini Index to file.
  data
    .slice(Over(Second), "label", true)
    .sampleWithValue(SampleByScore(), scores)
    .renameWithValue(renameWithScore, scores)
    .gini(Over(Second))
    .save(s"./demo.${output}/ensemble.gini.out")
}

