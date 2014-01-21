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
        val nextModel = LDAModel(
          DoubleMatrix.zeros(numDocs, 1),
          DoubleMatrix.zeros(numTopics, 1),
          DoubleMatrix.zeros(numDocs, numTopics),
          DoubleMatrix.zeros(numTopics, numTerms)
        )

        val parTopicAssign = currentParIter.map {
          case (Document(docIdx, content), zIdx) =>
            content.zip(zIdx).map {
              case (word, _) =>
                val curz = uniformDistSampler(numTopics)
                logInfo(s"Uniform sampling get $curz")
                updateOnce(nextModel, docIdx, word, curz, +1)
                logInfo(s"next model doc count is ${nextModel.docCounts.get(docIdx, 0)}")
                logInfo(s"next model topic count is ${nextModel.topicCounts.get(curz, 0)}")
                logInfo(s"next model doc topic count is ${nextModel.docTopicCounts.get(docIdx, curz)}")
                logInfo(s"next model topic term count is ${nextModel.topicTermCounts.get(curz, word)}")
                curz
            }
          case _ =>
            logInfo("Gee.. I don't know why")
            0
        }
        Seq(Pair(parTopicAssign, nextModel)).toIterator
    }

    topicAssign.unpersist(true)
    topicAssign = topicAssignAndParams.flatMap(x => x._1)
    topicAssign.cache

    val params = topicAssignAndParams.map(x => x._2).collect

    val dc = params.map(x => x.docCounts)
    val tc = params.map(x => x.topicCounts)
    val dtc = params.map(x => x.docTopicCounts)
    val ttc = params.map(x => x.topicTermCounts)

    val initialModel = LDAModel(
      dc.reduce(_ addi _),
      tc.reduce(_ addi _),
      dtc.reduce(_ addi _),
      ttc.reduce(_ addi _))

    // Gibbs sampling
    Iterator.iterate(initialModel) { current =>
        logInfo("role in gibbs sampling stage")
        val nextModel = LDAModel(
          DoubleMatrix.zeros(numDocs, 1),
          DoubleMatrix.zeros(numTopics, 1),
          DoubleMatrix.zeros(numDocs, numTopics),
          DoubleMatrix.zeros(numTopics, numTerms)
        )

        val topicAssignAndParams = data.zip(topicAssign).mapPartitions { currentParIter =>
            val parTopicAssign = currentParIter.map {
              case (Document(docIdx, content), zIdx) =>
                content.zip(zIdx).map {
                  case (word, prevz) =>
                    updateOnce(current, docIdx, word, prevz, -1)
                    val curz = dropOneDistSampler(
                      current,
                      docTopicSmoothing,
                      topicTermSmoothing,
                      numTopics,
                      numTerms,
                      word,
                      docIdx)
                    updateOnce(current, docIdx, word, curz, +1)
                    updateOnce(nextModel, docIdx, word, curz, +1)
                    curz
                }
            }
            Seq(Pair(parTopicAssign, nextModel)).toIterator
        }



        topicAssign.unpersist(true)
        topicAssign = topicAssignAndParams.flatMap(x => x._1)
        topicAssign.cache

        val params = topicAssignAndParams.map(x => x._2).collect

        val dc = params.map(x => x.docCounts)
        val tc = params.map(x => x.topicCounts)
        val dtc = params.map(x => x.docTopicCounts)
        val ttc = params.map(x => x.topicTermCounts)

        LDAModel(
          dc.reduce(_ addi _),
          tc.reduce(_ addi _),
          dtc.reduce(_ addi _),
          ttc.reduce(_ addi _))
    }.drop(1).take(numOuterIterations).last
  }
}

