//scalastyle:off

package org.apache.spark.examples

import java.util

import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext, SparkEnv}
import org.apache.spark.mllib.linalg.Vectors
import breeze.linalg.{DenseVector => BDV, SparseVector => BSV, Vector => BV}
import breeze.linalg._
import org.apache.spark.mllib.linalg.distributed.{BlockMatrix, CoordinateMatrix, IndexedRow, IndexedRowMatrix, MatrixEntry, RowMatrix}
import breeze.numerics._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.rdd.{RDD, ASYNCcontext}
import org.apache.spark.broadcast._
import org.apache.spark.ml.linalg
import org.apache.spark.streaming.StreamingContext

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
object SparkASAGAArray{

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
    val b= 100

    val conf = new SparkConf().setAppName("Gradient").setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val points = MLUtils.loadLibSVMFile(sc, fname).repartition(numPart)


    // check how balanced the data is:
    points.mapPartitionsWithIndex{case (i,rows) => Iterator((i,rows.size))}.reduceByKey((i,j)=>i+j).foreach(println(_))

    //var alpha = new IndexedRowMatrix()
    var alphaBar= BDV.zeros[Double](d)
    var w = BDV.zeros[Double](d)

    //val bucket =new ResultsRdd[DenseVector[Double]]
    val bucket =new ASYNCcontext[(ListBuffer[Long],DenseVector[Double],DenseVector[Double])]

    val startTime = System.currentTimeMillis()
    var k = 0
    var ts = 0
    var parRecs = 0
    var parIndex = 0
    var obj = 0.0
    val pointsIndexed = points.zipWithIndex.cache()

    // a map from iteration number to broadcastID
    val IterationMap = new mutable.HashMap[Int,Long]()
    // a map from index of points to iteration number
    val IndexMap = new mutable.HashMap[Long,Int]()

    //verification map
    // it maps the broadcast id to its value
    val VerifyArr :Array[Broadcast[DenseVector[Double]]] = new Array[Broadcast[BDV[Double]]](1000)
    //bbc.changeBroadcastID(9999)
    while (k < numIteration){

      //println("******** iteration "+k+"********")
      //val a = BroadcastWrapper(sc,w)
      val a = sc.broadcast(w)
      var alphai= BDV.zeros[Double](d)
      bucket.setCurrentTime(k)
      VerifyArr(k)=a
      //IterationMap.put(k,a.index)
      IterationMap.put(k,a.getBroadcastID().broadcastId)

      //VerifyArr(k)=w
      //VerifyInd(k)= a.index.toInt
      //VerifyMap.put(a.index,a.value)

      /*val x = scala.io.StdIn.readLine()
      val x2 = x.replace("[","")
      val x3 = x2.replace("]","")
      val indInput = x3.split(' ').toList.map(_.toLong)



      val pSampledBef = pointsIndexed.filter(x=>indInput.contains(x._2))
      val pSampled = pSampledBef.flatMap ( x => {
        val frq = indInput.count(_ == x._2)
        var b2: List[(LabeledPoint, Long)] = List.fill(frq)(x)
        b2

      }
      )*/

      val pSampled = pointsIndexed.sample(true,b.toFloat/N)
      //val IndexGrad = pSampled.map(x=>(x._2, gradfun(x._1,z(k).value)))
      //val te = IterationMap.get(k).get
      val te = a
      val latestIndex = a.getBroadcastID().broadcastId

      //TODO: fix set mode
      sc.set_mode(0)
      val SampledIndices = pSampled.map(x=>x._2).collect()

      val sampledMap = new mutable.HashMap[Long,Long]()

      for(j<-SampledIndices){
        if(IndexMap.contains(j)){

          val it_num = IndexMap.get(j).get
          val br_num = IterationMap.get(it_num).get
          sampledMap.put(j,br_num)
        }
        else{
          //TODO
        }

      }

      val IndexGrad = pSampled.map{x=>
        te.changeBroadcastID(latestIndex)

        val e2 = gradfun(x._1,te.value)
        var e3 = BDV.zeros[Double](d)
        if(sampledMap.contains(x._2)){
          val g = sampledMap.get(x._2).get
          te.changeBroadcastID(g)
          e3 =gradfun(x._1,te.value)
        }
        (x._2,e2,e3)
      }

      IndexGrad.ASYNCaggregate(new ListBuffer[Long](),BDV.zeros[Double](d),BDV.zeros[Double](d))((x, y)=>{x._1+=y._1;(x._1,x._2+y._2,x._3+y._3)}, (x, y)=>(x._1++y._1,x._2+y._2,x._3+y._3),bucket)

      while(bucket.getSize() <1){
        Thread.sleep(2)
        //println("bucket size is "+ bucket.size())

      }
      for (i<-1 to bucket.getSize()){
        // TODO: add parindex to grad updates (K1, K2, V)
        val info = bucket.getFromBucket()
        val data = bucket.getFromBucket().gettaskResult()
        val lis = data._1
        val gradient = data._2
        ts = info.getStaleness()
        alphai = data._3
        parRecs = info.getbatchSize()
        parIndex = info.getWorkerID()
        for(key<-lis){
          IndexMap.put(key,ts)
        }

        alphaBar :*= N.toDouble
        alphaBar = ( alphaBar -  alphai + gradient)
        alphaBar :*= (1.0/N)

        alphai :*= (2.0 /parRecs)
        gradient :*= (2.0 /parRecs)

        w = w - gamma * (gradient -alphai +alphaBar)

        k = k + 1


      }
      //Thread.sleep(1000)

      if(k%10 == 0){
        //te.setID(latestIndex)
        sc.set_mode(0)
        obj = points.map(p=> scala.math.pow((new BDV[Double](p.features.toArray).t *w  - p.label),2)).reduce(_ + _)
        obj=obj/N
        println("objective function : "+obj+" after "+ k+ " iterations")


        if(obj< -1 ){
          println("objective function : "+obj+" after "+ k+ " iterations")
          k=numIteration

        }
      }


    }

    val stopTime=System.currentTimeMillis()
    val elapsedTime = (stopTime-startTime)/1e3
    println("Elapsed Time: "+elapsedTime)
    println("The Final results is: ")
    println(w)

    Thread.sleep(1000)

  }
  def gradfun(p:LabeledPoint, w: DenseVector[Double]): DenseVector[Double] ={
    //println(w)
    val x = new BDV[Double](p.features.toArray)
    val y= p.label
    val grad = (x.t* w - y) * x
    //alpha(::,index.toInt) := grad

    //println("vec is :"+ w)
    grad
  }

  def gradfunSGD(p:LabeledPoint, w: DenseVector[Double],w2: DenseVector[Double]): DenseVector[Double] ={
    val x = new BDV[Double](p.features.toArray)
    //val hh = h.get(0).get
    val y= p.label
    val grad = (x.t* w - y) * x
    grad
  }







}
