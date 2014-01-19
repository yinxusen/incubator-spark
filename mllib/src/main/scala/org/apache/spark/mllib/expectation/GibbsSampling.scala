package org.apache.spark.mllib.expectation

import org.apache.spark.rdd.RDD
import breeze.linalg.{VectorBuilder, DenseMatrix, DenseVector, SparseVector}
import scala.util._
import org.apache.spark.mllib.clustering.Document

class GibbsSampling

object GibbsSampling {

  def uniformDistSampler(dimension: Int): Int = Random.nextInt(dimension)

  def runGibbsSampling(
      data: RDD[Document],
      numInnerIterations: Int,
      numTerms: Int,
      numDocs: Int,
      numTopics: Int,
      docTopicSmoothing: Double,
      termTopicSmoothing: Double) :
      (DenseVector[Int], DenseVector[Int], DenseMatrix[Int], DenseMatrix[Int]) = {

    data.cache

    val docCounts = DenseVector.zeros[Int](numDocs)
    val topicCounts = DenseVector.zeros[Int](numTopics)
    val docTopicCounts = DenseMatrix.zeros[Int](numDocs, numTopics)
    val termTopicCounts = DenseMatrix.zeros[Int](numTerms, numTopics)

    // initialisation
    data.mapPartitions { currentPartitionIter =>
      for (i <- 0 until numInnerIterations) {
        val currentPartitionData = currentPartitionIter.toArray
        currentPartitionData.map { case Document(fileIdx, content) =>
          while (content.hasNext) {
            val (word, topic) = content.next
            val z = uniformDistSampler(numTopics)
              docCounts(fileIdx) += 1
              topicCounts(z) += 1
              docTopicCounts(fileIdx, z) += 1
              termTopicCounts(topic, z) += 1
          }
          var offset = 0
          while (offset < content.activeSize) {
            val index = content.indexAt(offset)
            val value = content.valueAt(offset)
            for (j <- 0 until value) {

            }
            offset += 1
          }
        }
      }
      ???
    }
    ???
  }
}


// vim: set ts=4 sw=4 et:
