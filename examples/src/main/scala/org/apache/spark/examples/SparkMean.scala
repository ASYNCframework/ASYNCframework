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
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix, RowMatrix, BlockMatrix, CoordinateMatrix, MatrixEntry}
import breeze.numerics._
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.mllib.linalg.distributed.RowMatrix
object SparkMean{

  def main(args: Array[String]): Unit = {

    // inputs from user
    // later check if the file exists
    println("input format: [file name] [num columns] [num rows] [num partitions] [num iterations]")
       val fname = args(0)
       val d = args(1).toInt
       val N = args(2).toInt
       val numPart = args(3).toInt
       val numIteration = args(4).toInt

    val conf = new SparkConf().setAppName("Average").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val points = MLUtils.loadLibSVMFile(sc, fname).repartition(numPart)

    var w = BDV.zeros[Double](d)
    var average = 0.0
    // broadcast variable
    //var wbr = sc.broadcast(w)
    for (i <- 1 to numIteration){
      println("******** iteration "+i+"********")
      average = points.map(p=> sum(new BDV[Double](p.features.toArray))).reduce(_+_)
      average :*= 1.0/N
      //6w = wbr.value +:+ (average:*=0.5)
      //wbr = sc.broadcast(w)


    }

    println("The Final results is: ")
    println(average)



  }



}
