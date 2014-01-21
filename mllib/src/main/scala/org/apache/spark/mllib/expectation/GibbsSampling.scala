package org.apache.spark.mllib.expectation

import org.apache.spark.rdd.RDD
import org.jblas.DoubleMatrix
import scala.util._
import org.apache.spark.Logging
import org.apache.spark.mllib.clustering.{LDAModel, Document}
import breeze.util.Implicits._

class GibbsSampling

object GibbsSampling extends Logging {

  private def uniformDistSampler(dimension: Int): Int = Random.nextInt(dimension)

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
    val rightFrac = topicThisDoc.sum + numTopics * docTopicSmoothing - 1
    val leftFrac = model.topicCounts.add(numTerms * topicTermSmoothing)
    topicThisTerm.divi(leftFrac)
    topicThisDoc.divi(rightFrac)
    topicThisTerm.mul(topicThisDoc)
    topicThisTerm.divi(topicThisTerm.sum)
    val roulette = Random.nextDouble
    assert(roulette <= 1.0 && roulette >= 0.0)
    assert(math.abs(topicThisTerm.sum - 1.0) < 0.01)
    var sumNow: Double = 0.0
    var result: Int = 0
    for (i <- 0 until numTopics) {
      sumNow += topicThisTerm.get(i, 0)
      if (sumNow > roulette) {
        result = i
      }
    }
    result
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

    data.cache

    // construct topic assignment RDD
    var topicAssign = data.map {
      case Document(docIdx, content) =>
        content.map(_ => 0)
    }

    topicAssign.cache

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
    }

    topicAssign.unpersist(true)
    topicAssign = topicAssignAndParams.flatMap(x => x._1)
    topicAssign.cache

    val params = topicAssignAndParams.map(x => x._2).collect

    val initialModel = LDAModel(
      params.map(x => x.docCounts).reduce(_ addi _),
      params.map(x => x.topicCounts).reduce(_ addi _),
      params.map(x => x.docTopicCounts).reduce(_ addi _),
      params.map(x => x.topicTermCounts).reduce(_ addi _))

    logInfo(s"initial model doc count is ${initialModel.docCounts}")
    logInfo(s"initial model topic count is ${initialModel.topicCounts}")
    logInfo(s"initial model doc-topic count is ${initialModel.docTopicCounts}")
    logInfo(s"initial model topic-term count is ${initialModel.topicTermCounts}")


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
      }

      topicAssign.unpersist(true)
      topicAssign = topicAssignAndParams.flatMap(x => x._1)
      topicAssign.cache

      val params = topicAssignAndParams.map(x => x._2).collect

      LDAModel(
        params.map(x => x.docCounts).reduce(_ addi _),
        params.map(x => x.topicCounts).reduce(_ addi _),
        params.map(x => x.docTopicCounts).reduce(_ addi _),
        params.map(x => x.topicTermCounts).reduce(_ addi _))
    }.take(numOuterIterations).last
  }
}

