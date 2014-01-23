package org.apache.spark.mllib.expectation

import org.jblas.DoubleMatrix

import scala.util._
import scala.util.control.Breaks

import breeze.util.Implicits._

import org.apache.spark.rdd.RDD
import org.apache.spark.Logging
import org.apache.spark.mllib.clustering.{LDAModel, Document}

class GibbsSampling

object GibbsSampling extends Logging {

  val mybreaks = new Breaks

  /**
   * This is a uniform distribution sampler, which is only used for the initialization.
   * @param dimension
   * @return
   */
  private def uniformDistSampler(rand: Random, dimension: Int): Int = rand.nextInt(dimension)

  /**
   * This is a multinomial distribution sampler.
   * I use a roulette method to sample an Int back.
   * @param dist
   * @return
   */
  private def multinomialDistSampler(rand: Random, dist: DoubleMatrix): Int = {
    val dimension = dist.length
    val roulette = rand.nextDouble()
    var sumNow: Double = 0.0
    var result: Int = 0
    mybreaks.breakable {
      for (i <- 0 until dimension) {
        sumNow += dist.get(i, 0)
        if (sumNow > roulette) {
          result = i
          mybreaks.break()
        }
      }
    }
    result
  }

  /**
   * I use this function to compute the new distribution after drop one from current document.
   * This is a really essential part of Gibbs sampling for LDA, you can refer to the paper:
   * <I>Parameter estimation for text analysis</I>
   * In the end, I call multinomialDistSampler to sample a figure out.
   * @param model
   * @param docTopicSmoothing
   * @param topicTermSmoothing
   * @param numTopics
   * @param numTerms
   * @param termIdx
   * @param docIdx
   * @return
   */
  private def dropOneDistSampler(
      model: LDAModel,
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
   * This method is used for update LDAModel once.
   * Updater can be +1 or -1.
   * @param model
   * @param docIdx
   * @param word
   * @param curz
   * @param updater
   */
  private def updateOnce(model: LDAModel, docIdx: Int, word: Int, curz: Int, updater: Int) {
    model.docCounts.put(docIdx, 0, model.docCounts.get(docIdx, 0) + updater)
    model.topicCounts.put(curz, 0, model.topicCounts.get(curz, 0) + updater)
    model.docTopicCounts.put(docIdx, curz, model.docTopicCounts.get(docIdx, curz) + updater)
    model.topicTermCounts.put(curz, word, model.topicTermCounts.get(curz, word) + updater)
  }

  /**
   * Main function of running a Gibbs sampling method.
   * It contains two phases of total Gibbs sampling:
   * first is initialization, second is real sampling.
   * @param data
   * @param numOuterIterations
   * @param numInnerIterations
   * @param numTerms
   * @param numDocs
   * @param numTopics
   * @param docTopicSmoothing
   * @param topicTermSmoothing
   * @return
   */
  def runGibbsSampling(
      data: RDD[Document],
      numOuterIterations: Int,
      numInnerIterations: Int,
      numTerms: Int,
      numDocs: Int,
      numTopics: Int,
      docTopicSmoothing: Double,
      topicTermSmoothing: Double): LDAModel = {

    data.cache()

    // construct topic assignment RDD
    var topicAssign = data.map(x => x.content.map(_ => 0)).cache()

    logInfo("role in initialization mode")

    val topicAssignAndParams = data.zip(topicAssign).mapPartitions { currentParIter =>
      val currentParData = currentParIter.toArray

      val nextModel = LDAModel(
        DoubleMatrix.zeros(numDocs, 1),
        DoubleMatrix.zeros(numTopics, 1),
        DoubleMatrix.zeros(numDocs, numTopics),
        DoubleMatrix.zeros(numTopics, numTerms)
      )

      val parTopicAssign = currentParData.zipWithIndex.map { iter =>
        val curDoc = iter._1._1
        val zIdx = iter._1._2
        val rand = new Random(42+iter._2)
        curDoc.content.zip(zIdx).map { x =>
          val word = x._1
          val curz = uniformDistSampler(rand, numTopics)
          updateOnce(nextModel, curDoc.docIdx, word, curz, +1)
          curz
        }
      }
      Seq(Pair(parTopicAssign, nextModel)).iterator
    }

    topicAssign.unpersist()
    topicAssign = topicAssignAndParams.flatMap(x => x._1)
    topicAssign.cache()

    val params = topicAssignAndParams.map(x => x._2).collect()

    val initialModel = LDAModel(
      params.map(x => x.docCounts).reduce(_ addi _),
      params.map(x => x.topicCounts).reduce(_ addi _),
      params.map(x => x.docTopicCounts).reduce(_ addi _),
      params.map(x => x.topicTermCounts).reduce(_ addi _))

    // Gibbs sampling
    var randSeedSalt = 1
    Iterator.iterate(initialModel) { current =>
      logInfo("role in gibbs sampling stage")

      randSeedSalt += 1

      val topicAssignAndParams = data.zip(topicAssign).mapPartitions { currentParIter =>
        val currentParData = currentParIter.toArray

        val nextModel = LDAModel(
          DoubleMatrix.zeros(numDocs, 1),
          DoubleMatrix.zeros(numTopics, 1),
          DoubleMatrix.zeros(numDocs, numTopics),
          DoubleMatrix.zeros(numTopics, numTerms)
        )

        val parTopicAssign = currentParData.zipWithIndex.map { iter =>
          val curDoc = iter._1._1
          val zIdx = iter._1._2
          val rand = new Random(42 + randSeedSalt * numOuterIterations + iter._2)
          curDoc.content.zip(zIdx).map { x =>
            val word = x._1
            val prevz = x._2
            updateOnce(current, curDoc.docIdx, word, prevz, -1)
            val curz = dropOneDistSampler(
              current,
              docTopicSmoothing,
              topicTermSmoothing,
              numTopics,
              numTerms,
              word,
              curDoc.docIdx,
              rand)
            updateOnce(current, curDoc.docIdx, word, curz, +1)
            updateOnce(nextModel, curDoc.docIdx, word, curz, +1)
            curz
          }
        }
        Seq(Pair(parTopicAssign, nextModel)).toIterator
      }

      topicAssign.unpersist()
      topicAssign = topicAssignAndParams.flatMap(x => x._1)
      topicAssign.cache()

      val params = topicAssignAndParams.map(x => x._2).collect()

      current.copy(
        params.map(x => x.docCounts).reduce(_ addi _),
        params.map(x => x.topicCounts).reduce(_ addi _),
        params.map(x => x.docTopicCounts).reduce(_ addi _),
        params.map(x => x.topicTermCounts).reduce(_ addi _))
    }.drop(1).take(numOuterIterations).last
  }

  /**
   * We use LDAModel to infer parameters Phi and Theta.
   * Just a two simple equations.
   * @param model
   * @param numTopics
   * @param numTerms
   * @param docTopicSmoothing
   * @param topicTermSmoothing
   * @return
   */
  def solvePhiAndTheta(
      model: LDAModel,
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
   * Small perplexiy means good result.
   * @param data
   * @param phi
   * @param theta
   * @return
   */
  def perplexity(data: RDD[Document], phi: DoubleMatrix, theta: DoubleMatrix): Double = {
    val (pword, totalNum) = data.map { doc =>
      val currentTheta = theta.getRow(doc.docIdx).mmul(phi)
      doc.content.map(x => (math.log(currentTheta.get(x)), 1))
        .reduce((a, b) => (a._1 + b._1, a._2 + b._2))
    }.reduce((a, b) => (a._1 + b._1, a._2 + b._2))
    math.exp(-1 * pword / totalNum)
  }
}

