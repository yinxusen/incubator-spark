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

    val topicAssignment = new VectorBuilder[Int](Int.MaxValue, numTerms)

    // initialisation
    data.mapPartitions { currentPartitionIter =>
      val currentPartitionData = currentPartitionIter.toArray
      for (i <- 0 until numInnerIterations) {
        var wordCountsPerPartition = 0
        currentPartitionData.map { case (fileIdx, content) =>
          var offset = 0
          while (offset < content.activeSize) {
            val index = content.indexAt(offset)
            val value = content.valueAt(offset)
            for (j <- 0 until value) {
              val z = uniformDistSampler(numTopics)
              topicAssignment.add(wordCountsPerPartition, z)
              wordCountsPerPartition += 1
              docCounts(fileIdx) += 1
              topicCounts(z) += 1
              docTopicCounts(fileIdx, z) += 1
              termTopicCounts(index, z) += 1
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
