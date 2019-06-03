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
//scalastyle:off

package org.apache.spark.examples

import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vectors
import breeze.linalg.{DenseVector => BDV, SparseVector => BSV, Vector => BV}
import breeze.linalg._

import org.apache.spark.rdd.ResultsRdd

object SparkGradientSync {

  def main(args: Array[String]): Unit = {

    // inputs from user
    // later check if the file exists
    println("input format: [file name] [num columns] [num rows] [num partitions] [num iterations]")
    val fname = args(0)
    val d = args(1).toInt
    val N = args(2).toInt
    val numPart = args(3).toInt
    val numIteration = args(4).toInt
    val alpha = 0.01

    val conf = new SparkConf().setAppName("Gradient sync").setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val points = MLUtils.loadLibSVMFile(sc, fname).repartition(numPart)


    // check how balanced the data is:
    points.mapPartitionsWithIndex { case (i, rows) => Iterator((i, rows.size)) }.reduceByKey((i, j) => i + j).foreach(println(_))


    var w = BDV.zeros[Double](d)

    var gradient = BDV.zeros[Double](d)

    val bucket = new ResultsRdd[DenseVector[Double]]
    //points.setResultParam(f)
    //points.resultRddObj = f
    val startTime = System.currentTimeMillis()
    var k = 0
    var ts = 0

    var obj= 0.0
    while (k < numIteration) {
      //println("******** iteration " + k + "********")
      gradient = points.map(p => (new BDV[Double](p.features.toArray).t * w - p.label) * new BDV[Double](p.features.toArray)).reduce(_ + _)
      gradient :*= (2.0 / N)
      w = w - alpha * gradient
      k = k + 1


      if(k%10 == 0){
        obj = points.map(p=> scala.math.pow((new BDV[Double](p.features.toArray).t * w - p.label),2)).reduce(_ + _)
        obj=obj/N

        if(obj<8.77){
          println("objective function : "+obj+" after "+ k+ " iterations")
          k=numIteration

        }
      }



    }

    val stopTime = System.currentTimeMillis()
    val elapsedTime = (stopTime - startTime) / 1e3
    println("Elapsed Time: " + elapsedTime)
    println("The Final results is: ")
    println(w)

    Thread.sleep(1000)

  }


}

