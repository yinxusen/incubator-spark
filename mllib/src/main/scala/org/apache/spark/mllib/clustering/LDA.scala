package org.apache.spark.mllib.clustering

import org.jblas.DoubleMatrix

import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.expectation.GibbsSampling
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, Logging}

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
}

object LDA {

  def train(
      data: RDD[Document],
      numTopics: Int,
      docTopicSmoothing: Double,
      topicTermSmoothing: Double,
      numIterations: Int,
      numDocs: Int,
      numTerms: Int): (DoubleMatrix, DoubleMatrix) = {
    val lda = new LDA(numTopics,
      docTopicSmoothing,
      topicTermSmoothing,
      numIterations,
      numDocs,
      numTerms)
    val model = lda.run(data)
    GibbsSampling.
      solvePhiAndTheta(model, numTopics, numTerms, docTopicSmoothing, topicTermSmoothing)
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
    val (phi, theta) = LDA.train(data, k, 0.01, 0.01, iters, numDocs, numTerms)
    val pp = GibbsSampling.perplexity(data, phi, theta)
    println(s"final model Phi is ${phi}")
    println(s"final model Theta is ${theta}")
    println(s"final mode perplexity is ${pp}")
  }
}
