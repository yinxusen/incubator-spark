/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.mllib.optimization

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD

import org.jblas.DoubleMatrix

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
 * Class used to solve an optimization problem using Gradient Descent.
 * @param gradient Gradient function to be used.
 * @param updater Updater to be used to update weights after every iteration.
 */
class GradientDescent(var gradient: Gradient, var updater: Updater)
  extends Optimizer with Logging
{
  private var stepSize: Double = 1.0
  private var numIterations: Int = 100
  private val numOuterIterations: Int = 20
  private var regParam: Double = 0.0
  private var miniBatchFraction: Double = 1.0

  /**
   * Set the step size per-iteration of SGD. Default 1.0.
   */
  def setStepSize(step: Double): this.type = {
    this.stepSize = step
    this
  }

  /**
   * Set fraction of data to be used for each SGD iteration. Default 1.0.
   */
  def setMiniBatchFraction(fraction: Double): this.type = {
    this.miniBatchFraction = fraction
    this
  }

  /**
   * Set the number of iterations for SGD. Default 100.
   */
  def setNumIterations(iters: Int): this.type = {
    this.numIterations = iters
    this
  }

  /**
   * Set the regularization parameter used for SGD. Default 0.0.
   */
  def setRegParam(regParam: Double): this.type = {
    this.regParam = regParam
    this
  }

  /**
   * Set the gradient function to be used for SGD.
   */
  def setGradient(gradient: Gradient): this.type = {
    this.gradient = gradient
    this
  }


  /**
   * Set the updater function to be used for SGD.
   */
  def setUpdater(updater: Updater): this.type = {
    this.updater = updater
    this
  }

  def optimize(data: RDD[(Double, Array[Double])], initialWeights: Array[Double])
    : Array[Double] = {

    assert(numIterations >= numOuterIterations)

    val (weights, stochasticLossHistory) = GradientDescent.runMiniBatchSGD(
        data,
        gradient,
        updater,
        stepSize,
        numOuterIterations,
        numIterations / numOuterIterations,
        regParam,
        miniBatchFraction,
        initialWeights)
    weights
  }

}

// Top-level method to run gradient descent.
object GradientDescent extends Logging {
//  def runMiniBatchSGD(
//    data: RDD[(Double, Array[Double])],
//    gradient: Gradient,
//    updater: Updater,
//    stepSize: Double,
//    numIterations: Int,
//    regParam: Double,
//    miniBatchFraction: Double,
//    initialWeights: Array[Double]) : (Array[Double], Array[Double]) = {
//
//    val stochasticLossHistory = new ArrayBuffer[Double](numIterations)
//
//    val nexamples: Long = data.count()
//    val miniBatchSize = nexamples * miniBatchFraction
//
//    // Initialize weights as a column vector
//    var weights = new DoubleMatrix(initialWeights.length, 1, initialWeights:_*)
//    var regVal = 0.0
//
//    for (i <- 1 to numIterations) {
//      val (gradientSum, lossSum) = data.sample(false, miniBatchFraction, 42+i).map {
//        case (y, features) =>
//          val featuresCol = new DoubleMatrix(features.length, 1, features:_*)
//          val (grad, loss) = gradient.compute(featuresCol, y, weights)
//          (grad, loss)
//      }.reduce((a, b) => (a._1.addi(b._1), a._2 + b._2))
//
//      /**
//       * NOTE(Xinghao): lossSum is computed using the weights from the previous iteration
//       * and regVal is the regularization value computed in the previous iteration as well.
//       */
//      stochasticLossHistory.append(lossSum / miniBatchSize + regVal)
//      val update = updater.compute(
//        weights, gradientSum.div(miniBatchSize), stepSize, i, regParam)
//      weights = update._1
//      regVal = update._2
//    }
//
//    logInfo("GradientDescent finished. Last 10 stochastic losses %s".format(
//      stochasticLossHistory.takeRight(10).mkString(", ")))
//
//    (weights.toArray, stochasticLossHistory.toArray)
//  }

  def runMiniBatchSGD(
      data: RDD[(Double, Array[Double])],
      gradient: Gradient,
      updater: Updater,
      stepSize: Double,
      numInnerIterations: Int,
      numOuterIterations: Int,
      regParam: Double,
      miniBatchFraction: Double,
      initialWeights: Array[Double]) : (Array[Double], Array[Double]) = {

    val stochasticLossHistory = new ArrayBuffer[Double](numOuterIterations)

    val nexamples: Long = data.count()
    val numPartition = data.partitions.length
    val miniBatchSize = nexamples * miniBatchFraction / numPartition

    // Initialize weights as a column vector
    var weights = new DoubleMatrix(initialWeights.length, 1, initialWeights: _*)
    var regVal = 0.0

    val timeArray = new ArrayBuffer[Long](numOuterIterations)
    for (i <- 1 to numOuterIterations) {
      val begin = System.nanoTime
      val x = data.mapPartitions { iter =>
        val xxx = iter.toArray

        val localLossHistory = new ArrayBuffer[Double](numInnerIterations)

        for (j <- 1 to numInnerIterations) {
          val rand = new Random(42 + i * numOuterIterations + j)
          val (gradientSum, lossSum) = xxx.filter(x => rand.nextDouble() <= miniBatchFraction).map { case (y, features) =>
            val featuresCol = new DoubleMatrix(features.length, 1, features: _*)
            val (grad, loss) = gradient.compute(featuresCol, y, weights)
            (grad, loss)
          }.reduce((a, b) => (a._1.addi(b._1), a._2 + b._2))

          localLossHistory.append(lossSum / miniBatchSize + regVal)
          val update = updater.compute(weights, gradientSum.div(miniBatchSize), stepSize, (i - 1) + numOuterIterations + j, regParam)
          weights = update._1
          regVal = update._2
        }

        Seq((weights, localLossHistory.toArray)).toIterator
      }

      val c = x.collect()
      val (ws, hs) = c.unzip

      stochasticLossHistory.append(hs.head.reduce(_ + _) / hs.head.size)

      val weightsSum = ws.reduce(_ addi _)
      weights = weightsSum.divi(c.size)
      val end = System.nanoTime
      timeArray.append(end-begin)
    }

    logInfo("GradientDescent finished. Last 10 stochastic losses %s".format(
      stochasticLossHistory.mkString(", ")))
    logInfo("time count in array is %s".format(timeArray.mkString(", ")))

    (weights.toArray, stochasticLossHistory.toArray)
  }
}
