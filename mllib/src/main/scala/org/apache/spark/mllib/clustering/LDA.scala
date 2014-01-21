package org.apache.spark.mllib.clustering

import org.apache.spark.{SparkContext, Logging}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.jblas.DoubleMatrix
import org.apache.spark.mllib.expectation.GibbsSampling

case class LDAModel (
    val docCounts: DoubleMatrix,
    val topicCounts: DoubleMatrix,
    val docTopicCounts: DoubleMatrix,
    val topicTermCounts: DoubleMatrix)
  extends Serializable

class LDA private (
    var numTopics: Int,
    var docTopicSmoothing: Double,
    var topicTermSmoothing: Double,
    var numIteration: Int,
    var numDocs: Int,
    var numTerms: Int)
  extends Serializable with Logging
{
  def run(input: RDD[Document]): LDAModel = {
    GibbsSampling.runGibbsSampling(
      input,
      numIteration,
      1,
      numTerms,
      numDocs,
      numTopics,
      docTopicSmoothing,
      topicTermSmoothing)
  }

  def solvePhiAndTheta(finalModel: LDAModel): (DoubleMatrix, DoubleMatrix) = {
    finalModel.docCounts.addi(docTopicSmoothing * numTopics)
    finalModel.topicCounts.addi(topicTermSmoothing * numTerms)
    finalModel.docTopicCounts.addi(docTopicSmoothing)
    finalModel.topicTermCounts.addi(topicTermSmoothing)
    (finalModel.topicTermCounts.divRowVector(finalModel.topicCounts),
      finalModel.docTopicCounts.divRowVector(finalModel.docCounts))
  }
}

object LDA {

  def train(
      data: RDD[Document],
      numTopics: Int,
      docTopicSmoothing: Double,
      topicTermSmoothing: Double,
      numIterations: Int,
      numDocs: Int,
      numTerms: Int): LDAModel = {
    val lda = new LDA(numTopics,
      docTopicSmoothing,
      topicTermSmoothing,
      numIterations,
      numDocs,
      numTerms)
    lda.run(data)
    // lda.solvePhiAndTheta(finalModel)
  }

  def main(args: Array[String]) {
    if (args.length != 5) {
      println("Usage: LDA <master> <input_dir> <k> <max_iterations> <mini-split>")
      System.exit(1)
    }

    val (master, inputDir, k, iters, minSplit) =
      (args(0), args(1), args(2).toInt, args(3).toInt, args(4).toInt)
    val sc = new SparkContext(master, "LDA")
    val (data, wordMap, docMap) = MLUtils.loadCorpus(sc, inputDir, minSplit)
    val numDocs = docMap.size
    val numTerms = wordMap.size
    val model = LDA.train(data, k, 0.01, 0.01, iters, numDocs, numTerms)
    println(s"initial model doc count is ${model.docCounts}")
    println(s"initial model topic count is ${model.topicCounts}")
    println(s"initial model doc-topic count is ${model.docTopicCounts}")
    println(s"initial model topic-term count is ${model.topicTermCounts}")

  }
}
