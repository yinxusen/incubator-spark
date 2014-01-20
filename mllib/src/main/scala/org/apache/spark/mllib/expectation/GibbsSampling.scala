package org.apache.spark.mllib.expectation

import org.apache.spark.rdd.RDD
import org.jblas.DoubleMatrix
import breeze.util.Implicits._
import scala.util._
import org.apache.spark.mllib.clustering.{LDAModel, Document}

class GibbsSampling

object GibbsSampling {

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


    val docCounts = DoubleMatrix.zeros(numDocs, 1)
    val topicCounts = DoubleMatrix.zeros(numTopics, 1)
    val docTopicCounts = DoubleMatrix.zeros(numDocs, numTopics)
    val topicTermCounts = DoubleMatrix.zeros(numTopics, numTerms)

    case class fiveElement(
        topic: Iterator[Array[Int]],
        dc: DoubleMatrix,
        tc: DoubleMatrix,
        dtc: DoubleMatrix,
        ttc: DoubleMatrix)

    case class fourElement(
        dc: DoubleMatrix,
        tc: DoubleMatrix,
        dtc: DoubleMatrix,
        ttc: DoubleMatrix)

    // Gibbs sampling
    Iterator.iterate(LDAModel(docCounts, topicCounts, docTopicCounts, topicTermCounts)) {
      case LDAModel(_, _, _, _, true) =>

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
              case (word, _) =>
                val curz = uniformDistSampler(numTopics)
                nextModel.docCounts.put(docIdx, 0, nextModel.docCounts.get(docIdx, 0) + 1)
                nextModel.topicCounts.put(curz, 0, nextModel.topicCounts.get(curz, 0) + 1)
                nextModel.docTopicCounts.put(docIdx, curz, nextModel.docTopicCounts.get(docIdx, curz) + 1)
                nextModel.topicTermCounts.put(curz, word, nextModel.topicTermCounts.get(curz, word) + 1)
                curz
            }
          }
          Seq(fiveElement(
            parTopicAssign,
            docCounts,
            topicCounts,
            docTopicCounts,
            topicTermCounts)).toIterator
        }

        topicAssign.unpersist(true)
        topicAssign = topicAssignAndParams.flatMap {
          case fiveElement(topic, _, _, _, _) => topic
        }
        topicAssign.cache

        val params = topicAssignAndParams.map {
          case fiveElement(_, dc, tc, dtc, ttc) => fourElement(dc, tc, dtc, ttc)
        }.collect

        val dc = params.map {
          case fourElement(x, _, _, _) => x
        }
        val tc = params.map {
          case fourElement(_, x, _, _) => x
        }
        val dtc = params.map {
          case fourElement(_, _, x, _) => x
        }
        val ttc = params.map {
          case fourElement(_, _, _, x) => x
        }

        LDAModel(
          dc.reduce(_ addi _),
          tc.reduce(_ addi _),
          dtc.reduce(_ addi _),
          ttc.reduce(_ addi _),
          false)

      case current =>

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
                current.docCounts.put(docIdx, 0, current.docCounts.get(docIdx, 0) - 1)
                current.topicCounts.put(prevz, 0, current.topicCounts.get(prevz, 0) - 1)
                current.docTopicCounts.put(docIdx, prevz, current.docTopicCounts.get(docIdx, prevz) - 1)
                current.topicTermCounts.put(prevz, word, current.topicTermCounts.get(prevz, word) - 1)

                val curz = dropOneDistSampler(
                  current,
                  docTopicSmoothing,
                  topicTermSmoothing,
                  numTopics,
                  numTerms,
                  word,
                  docIdx)

                nextModel.docCounts.put(docIdx, 0, nextModel.docCounts.get(docIdx, 0) + 1)
                nextModel.topicCounts.put(curz, 0, nextModel.topicCounts.get(curz, 0) + 1)
                nextModel.docTopicCounts.put(docIdx, curz, nextModel.docTopicCounts.get(docIdx, curz) + 1)
                nextModel.topicTermCounts.put(curz, word, nextModel.topicTermCounts.get(curz, word) + 1)
                curz
            }
          }
          Seq(fiveElement(
            parTopicAssign,
            docCounts,
            topicCounts,
            docTopicCounts,
            topicTermCounts)).toIterator
        }



        topicAssign.unpersist(true)
        topicAssign = topicAssignAndParams.flatMap {
          case fiveElement(topic, _, _, _, _) => topic
        }
        topicAssign.cache

        val params = topicAssignAndParams.map {
          case fiveElement(_, dc, tc, dtc, ttc) => fourElement(dc, tc, dtc, ttc)
        }.collect

        val dc = params.map {
          case fourElement(x, _, _, _) => x
        }
        val tc = params.map {
          case fourElement(_, x, _, _) => x
        }
        val dtc = params.map {
          case fourElement(_, _, x, _) => x
        }
        val ttc = params.map {
          case fourElement(_, _, _, x) => x
        }

        current.copy(
          dc.reduce(_ addi _),
          tc.reduce(_ addi _),
          dtc.reduce(_ addi _),
          ttc.reduce(_ addi _),
          false)
    }.drop(1).take(numOuterIterations).last
  }
}

