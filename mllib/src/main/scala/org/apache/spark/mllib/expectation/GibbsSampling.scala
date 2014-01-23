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

  private def uniformDistSampler(dimension: Int): Int = Random.nextInt(dimension)

  private def multinomialDistSampler(dist: DoubleMatrix): Int = {
    val dimension = dist.length
    val roulette = Random.nextDouble()
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

  private def dropOneDistSampler(
      model: LDAModel,
      docTopicSmoothing: Double,
      topicTermSmoothing: Double,
      numTopics: Int,
      numTerms: Int,
      termIdx: Int,
      docIdx: Int): Int = {
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
    multinomialDistSampler(topicThisTerm)
  }

  private def updateOnce(model: LDAModel, docIdx: Int, word: Int, curz: Int, updater: Int) {
    model.docCounts.put(docIdx, 0, model.docCounts.get(docIdx, 0) + updater)
    model.topicCounts.put(curz, 0, model.topicCounts.get(curz, 0) + updater)
    model.docTopicCounts.put(docIdx, curz, model.docTopicCounts.get(docIdx, curz) + updater)
    model.topicTermCounts.put(curz, word, model.topicTermCounts.get(curz, word) + updater)
  }

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

      val parTopicAssign = currentParData.map { iter =>
        val curDoc = iter._1
        val zIdx = iter._2
        curDoc.content.zip(zIdx).map { x =>
          val word = x._1
          val curz = uniformDistSampler(numTopics)
          updateOnce(nextModel, curDoc.docIdx, word, curz, +1)
          curz
        }
      }
      Seq(Pair(parTopicAssign, nextModel)).iterator
    }.cache()

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
    Iterator.iterate(initialModel) { current =>
      logInfo("role in gibbs sampling stage")

      val topicAssignAndParams = data.zip(topicAssign).mapPartitions { currentParIter =>
        val currentParData = currentParIter.toArray

        val nextModel = LDAModel(
          DoubleMatrix.zeros(numDocs, 1),
          DoubleMatrix.zeros(numTopics, 1),
          DoubleMatrix.zeros(numDocs, numTopics),
          DoubleMatrix.zeros(numTopics, numTerms)
        )

        val parTopicAssign = currentParData.map { iter =>
          val curDoc = iter._1
          val zIdx = iter._2
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
              curDoc.docIdx)
            updateOnce(current, curDoc.docIdx, word, curz, +1)
            updateOnce(nextModel, curDoc.docIdx, word, curz, +1)
            curz
          }
        }
        Seq(Pair(parTopicAssign, nextModel)).toIterator
      }.cache()

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
}

