package org.apache.spark.mllib.expectation

import org.jblas.DoubleMatrix

import scala.util._

import org.apache.spark.rdd.RDD
import org.apache.spark.Logging
import org.apache.spark.mllib.clustering.{LDAParams, Document}

class GibbsSampling

object GibbsSampling extends Logging {

  /**
   * This is a uniform distribution sampler, which is only used for the initialization.
   */
  private def uniformDistSampler(rand: Random, dimension: Int): Int = rand.nextInt(dimension)

  /**
   * This is a multinomial distribution sampler.
   * I use a roulette method to sample an Int back.
   */
  private def multinomialDistSampler(rand: Random, dist: DoubleMatrix): Int = {
    val roulette = rand.nextDouble()

    def loop(index: Int, accum: Double): Int = {
      val sum = accum + dist.get(index)
      if (sum >= roulette) index else loop(index + 1, sum)
    }

    loop(0, 0.0)
  }

  /**
   * I use this function to compute the new distribution after drop one from current document.
   * This is a really essential part of Gibbs sampling for LDA, you can refer to the paper:
   * <I>Parameter estimation for text analysis</I>
   * In the end, I call multinomialDistSampler to sample a figure out.
   */
  private def dropOneDistSampler(
      model: LDAParams,
      docTopicSmoothing: Double,
      topicTermSmoothing: Double,
      numTopics: Int,
      numTerms: Int,
      termIdx: Int,
      docIdx: Int,
      rand: Random): Int = {
    val topicThisTerm = new DoubleMatrix(numTopics, 1)
    val topicThisDoc = new DoubleMatrix(numTopics, 1)
    model.topicTermCounts.getColumn(termIdx, topicThisTerm)
    model.docTopicCounts.getRow(docIdx, topicThisDoc)
    topicThisTerm.addi(topicTermSmoothing)
    topicThisDoc.addi(docTopicSmoothing)
    val fraction = model.topicCounts.add(numTerms * topicTermSmoothing)
    topicThisTerm.divi(fraction)
    topicThisTerm.muli(topicThisDoc)
    topicThisTerm.divi(topicThisTerm.sum)
    multinomialDistSampler(rand, topicThisTerm)
  }

  /**
   * Main function of running a Gibbs sampling method.
   * It contains two phases of total Gibbs sampling:
   * first is initialization, second is real sampling.
   */
  def runGibbsSampling(
      data: RDD[Document],
      numOuterIterations: Int,
      numInnerIterations: Int,
      numTerms: Int,
      numDocs: Int,
      numTopics: Int,
      docTopicSmoothing: Double,
      topicTermSmoothing: Double): LDAParams = {

    // construct topic assignment RDD
    logInfo("role in initialization mode")

    val assignedTopicsAndParams = data.mapPartitionsWithIndex { case (index, iterator) =>
      val params = LDAParams(numDocs, numTopics, numTerms)
      val rand = new Random(42 + index)
      val assignedTopics = iterator.flatMap { case Document(docId, content) =>
        content.map { term =>
          val topic = uniformDistSampler(rand, numTopics)
          params.inc(docId, term, topic)
          topic
        }
      }.toArray

      Seq((assignedTopics, params)).iterator
    }

    val initialAssignedTopics = assignedTopicsAndParams.map(_._1).cache()
    val params = assignedTopicsAndParams.map(_._2)
    val initialParams = params.reduce(_ addi _)

    // Gibbs sampling
    Iterator.iterate((initialParams, initialAssignedTopics, 0)) { case (lastParams, lastAssignedTopics, salt) =>
      logInfo("Start Gibbs sampling")

      val assignedTopicsAndParams = data.zip(lastAssignedTopics).mapPartitions { iter =>
        val params = LDAParams(numDocs, numTopics, numTerms)
        val rand = new Random(42 + salt * numOuterIterations)
        val assignedTopics = iter.map { case (Document(docId, content), topics) =>
          content.zip(topics).map { case (term, topic) =>
            lastParams.dec(docId, term, topic)
            val assignedTopic = dropOneDistSampler(lastParams, docTopicSmoothing,
              topicTermSmoothing, numTopics, numTerms, term, docId, rand)

            lastParams.inc(docId, term, assignedTopic)
            params.inc(docId, term, assignedTopic)
            assignedTopic
          }
        }.toArray

        Seq((assignedTopics, params)).iterator
      }

      val assignedTopics = assignedTopicsAndParams.flatMap(_._1).cache()
      val params = assignedTopicsAndParams.map(_._2).reduce(_ addi _)
      lastAssignedTopics.unpersist()

      (params, assignedTopics, salt + 1)
    }.drop(1 + numOuterIterations).next()._1
  }

  /**
   * We use LDAModel to infer parameters Phi and Theta.
   * Just a two simple equations.
   */
  def solvePhiAndTheta(
      model: LDAParams,
      numTopics: Int,
      numTerms: Int,
      docTopicSmoothing: Double,
      topicTermSmoothing: Double): (DoubleMatrix, DoubleMatrix) = {
    val docCnt = model.docCounts.add(docTopicSmoothing * numTopics)
    val topicCnt = model.topicCounts.add(topicTermSmoothing * numTerms)
    val docTopicCnt = model.docTopicCounts.add(docTopicSmoothing)
    val topicTermCnt = model.topicTermCounts.add(topicTermSmoothing)
    (topicTermCnt.divColumnVector(topicCnt),
      docTopicCnt.divColumnVector(docCnt))
  }

  /**
   * Perplexity is a kind of evaluation method of LDA. Usually it is used on unseen data.
   * But here we use it for current documents, which is also OK.
   * If using it on unseen data, you must do an iteration of Gibbs sampling before calling this.
   * Small perplexity means good result.
   */
  def perplexity(data: RDD[Document], phi: DoubleMatrix, theta: DoubleMatrix): Double = {
    val (pTerm, totalNum) = data.map { doc =>
      val currentTheta = theta.getRow(doc.docId).mmul(phi)
      doc.content.map(x => (math.log(currentTheta.get(x)), 1))
        .reduce((a, b) => (a._1 + b._1, a._2 + b._2))
    }.reduce((a, b) => (a._1 + b._1, a._2 + b._2))
    math.exp(-1 * pTerm / totalNum)
  }
}

