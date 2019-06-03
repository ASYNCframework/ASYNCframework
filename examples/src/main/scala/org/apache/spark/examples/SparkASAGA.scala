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
import org.apache.spark.mllib.linalg.distributed.{BlockMatrix, CoordinateMatrix, IndexedRow, IndexedRowMatrix, MatrixEntry, RowMatrix}
import breeze.numerics._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.rdd.{RDD, ResultsRdd}

import scala.collection.mutable.ListBuffer
object SparkASAGA{

  def main(args: Array[String]): Unit = {

    // inputs from user
    // later check if the file exists
    println("input format: [file name] [num columns] [num rows] [num partitions] [num iterations]")
    val fname = args(0)
    val d = args(1).toInt
    val N = args(2).toInt
    val numPart = args(3).toInt
    val numIteration = args(4).toInt
    val gamma=0.01
    val taw = 8
    var b= 1000

    val conf = new SparkConf().setAppName("Gradient").setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    sc.setCheckpointDir("/home/saeeds/SparkChecks")
    val points = MLUtils.loadLibSVMFile(sc, fname).repartition(numPart)


    // check how balanced the data is:
    points.mapPartitionsWithIndex{case (i,rows) => Iterator((i,rows.size))}.reduceByKey((i,j)=>i+j).foreach(println(_))

    //var alpha = new IndexedRowMatrix()
    var alphaSum = BDV.zeros[Double](d)
    var alphai= BDV.zeros[Double](d)
    var w = BDV.zeros[Double](d)

    var gradient = BDV.zeros[Double](d)

    val bucket =new ResultsRdd[DenseVector[Double]]
    //points.setResultParam(f)
    //points.resultRddObj = f
    val startTime = System.currentTimeMillis()
    var k = 0
    var ts = 0
    var parRecs = 0
    var parIndex = 0
    var obj = 0.0
    val pointsIndexed = points.zipWithIndex.cache()
    bucket.setCurrentTime(k)
    var History = sc.emptyRDD[(Long, DenseVector[Double])]
    while (k < numIteration){
      //println("******** iteration "+k+"********")

      val pSampled = pointsIndexed.sample(true,b.toFloat/N)
      val partitionInfo = pSampled.mapPartitionsWithIndex((index,it)=> Iterator((index, it)))
      //val partitionInfo = pSampled.mapPartitionsWithIndex((index,it)=> Iterator((index, it)))


      val IndexGrad = pSampled.map{case (p, index)=> (index, gradfun(p,w))}
      IndexGrad.map(_._2).reduce_async(_+_, bucket)


      while(bucket.getSize() <8){
        Thread.sleep(10)
        //println("bucket size is "+ bucket.size())

      }
      for (i<-1 to bucket.getSize()){

        // TODO: add parindex to grad updates (K1, K2, V)
        val info = bucket.getFromBucket()
        gradient = info.getData()
        ts = info.getTimeStamp()
        parRecs = info.getRecordsNum()
        parIndex = info.getPartitionIndex()

        //val a = partitionInfo.filter(x=>x._1==parIndex).flatMap(x=>x._2.toList.unzip._2).map(x=>(x,1))
        val a = List[Long](1,2,3,4,5,6,7,8,9,10)

        try{
          //TODO: I need to change the bucket so it contains the list of indices
          // In this case it will be on driver as list
            //alphai = History.filter(x=>a.contains(x._1)).map(x=>x._2).reduce(_+_)
            alphai = History.filter(x=>a.contains(x._1)).map(x=>x._2).reduce(_+_)

          //
            //alphai = BDV.zeros[Double](d)
          }
          catch {
            case e:NullPointerException => println("WTF!")
            case e: Exception => {alphai=BDV.zeros[Double](d)
            }
          }

        alphaSum :*= N.toDouble
        alphaSum = ( alphaSum -  alphai + gradient)
        alphaSum :*= (1.0/N)

        alphai :*= (2.0 /parRecs)
        gradient :*= (2.0 /parRecs)
         // w = w - gamma * (gradient -alphai +alphaSum)
        w = w - gamma * (gradient )

        k = k + 1
          bucket.setCurrentTime(k)

        // update history:
//        History.subtractByKey(IndexGrad).count()

      }
      History =History.subtractByKey(IndexGrad).union(IndexGrad)

      //History =History.union(IndexGrad)




      if(k%8 == 0){
        obj = points.map(p=> scala.math.pow((new BDV[Double](p.features.toArray).t * w - p.label),2)).reduce(_ + _)
        obj=obj/N

        if(obj<8.77 ||2>1){
          println("objective function : "+obj+" after "+ k+ " iterations")
          //k=numIteration

        }
      }
      //println("UpdateList size "+bucket.UpdateList.size)


    }

    val stopTime=System.currentTimeMillis()
    val elapsedTime = (stopTime-startTime)/1e3
    println("Elapsed Time: "+elapsedTime)
    println("The Final results is: ")
    println(w)

    Thread.sleep(1000)

  }
  def gradfun(p:LabeledPoint, w: DenseVector[Double]): DenseVector[Double] ={
    val x = new BDV[Double](p.features.toArray)
    val y= p.label
    val grad = (x.t* w - y) * x
    //alpha(::,index.toInt) := grad

    //println("alpha size is :"+ alpha.size)
    grad
  }

  def gradfunSGD(p:LabeledPoint, w: DenseVector[Double]): DenseVector[Double] ={
    val x = new BDV[Double](p.features.toArray)
    val y= p.label
    val grad = (x.t* w - y) * x
    grad
  }



}
