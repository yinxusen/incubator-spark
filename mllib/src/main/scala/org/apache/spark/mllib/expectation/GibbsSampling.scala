package org.apache.spark.mllib.expectation

import org.apache.spark.rdd.RDD
import org.jblas.DoubleMatrix
import scala.util._
import org.apache.spark.mllib.clustering.{LDAModel, Document}

class GibbsSampling

object GibbsSampling {

  def uniformDistSampler(dimension: Int): Int = Random.nextInt(dimension)

  def runGibbsSampling(
                        data: RDD[Document],
                        numOuterIterations: Int,
                        numInnerIterations: Int,
                        numTerms: Int,
                        numDocs: Int,
                        numTopics: Int,
                        docTopicSmoothing: Double,
                        termTopicSmoothing: Double): LDAModel = {

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
    val termTopicCounts = DoubleMatrix.zeros(numTerms, numTopics)

    case class fiveElement(topic: Array[Int], dc: DoubleMatrix, tc: DoubleMatrix, dtc: DoubleMatrix, ttc: DoubleMatrix)
    case class fourElement(dc: DoubleMatrix, tc: DoubleMatrix, dtc: DoubleMatrix, ttc: DoubleMatrix)

    // Gibbs sampling
    Iterator.iterate(LDAModel(docCounts, topicCounts, docTopicCounts, termTopicCounts)) {
      case LDAModel(_, _, _, _, true) =>

        val nextModel = LDAModel(
          DoubleMatrix.zeros(numDocs, 1),
          DoubleMatrix.zeros(numTopics, 1),
          DoubleMatrix.zeros(numDocs, numTopics),
          DoubleMatrix.zeros(numTerms, numTopics)
        )

        val topicAssignAndParams = data.zip(topicAssign).mapPartitions {
          case (Document(docIdx, content), zIdx) =>
            val curZIdxs = content.zip(zIdx).map {
              case (word, _) =>
                val curz = uniformDistSampler(numTopics)
                nextModel.docCounts.get(docIdx, 1) += 1
                nextModel.topicCounts.get(curz, 1) += 1
                nextModel.docTopicCounts.get(docIdx, curz) += 1
                nextModel.termTopicCounts.get(word, curz) += 1
                curz
            }
            Seq(fiveElement(curZIdxs, docCounts, topicCounts, docTopicCounts, termTopicCounts)).toIterator
        }

        topicAssign.unpersist(true)
        topicAssign = topicAssignAndParams.map {
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

        LDAModel(dc.reduce(_ addi _), tc.reduce(_ addi _), dtc.reduce(_ addi _), ttc.reduce(_ addi _), false)

      case current =>

        val nextModel = LDAModel(
          DoubleMatrix.zeros(numDocs, 1),
          DoubleMatrix.zeros(numTopics, 1),
          DoubleMatrix.zeros(numDocs, numTopics),
          DoubleMatrix.zeros(numTerms, numTopics)
        )

        val topicAssignAndParams = data.zip(topicAssign).mapPartitions {
          case (Document(docIdx, content), zIdx) =>
            val curZIdxs = content.zip(zIdx).map {
              case (word, prevz) =>
                current.docCounts.get(docIdx, 1) -= 1
                current.topicCounts.get(prevz, 1) -= 1
                current.docTopicCounts.get(docIdx, prevz) -= 1
                current.termTopicCounts.get(word, prevz) -= 1

                val curz = uniformDistSampler(numTopics)

                nextModel.docCounts.get(docIdx, 1) += 1
                nextModel.topicCounts.get(curz, 1) += 1
                nextModel.docTopicCounts.get(docIdx, curz) += 1
                nextModel.termTopicCounts.get(word, curz) += 1
                curz
            }
            Seq(fiveElement(curZIdxs, docCounts, topicCounts, docTopicCounts, termTopicCounts)).toIterator
        }

        topicAssign.unpersist(true)
        topicAssign = topicAssignAndParams.map {
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

        current.copy(dc.reduce(_ addi _), tc.reduce(_ addi _), dtc.reduce(_ addi _), ttc.reduce(_ addi _), false)
    }.drop(1).take(numOuterIterations)
  }
}


// vim: set ts=4 sw=4 et:
