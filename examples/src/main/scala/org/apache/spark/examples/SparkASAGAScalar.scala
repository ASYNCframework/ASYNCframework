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
object SparkASAGAScalar{

  def main(args: Array[String]): Unit = {

    // inputs from user
    // later check if the file exists
    println("input format: [file name] [num columns] [num rows] [num partitions] [num iterations]")
    val fname = args(0)
    val d = args(1).toInt
    val N = args(2).toInt
    val numPart = args(3).toInt
    val numIteration = args(4).toInt
    val gamma=1e-7/8
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
    val bucket =new ASYNCcontext[(ListBuffer[(Long,Double)],DenseVector[Double],DenseVector[Double])]

    val startTime = System.currentTimeMillis()
    var k = 0
    var ts = 0
    var parRecs = 0
    var parIndex = 0
    var obj = 0.0
    val pointsIndexed = points.zipWithIndex.cache()



    val ScalarMap= new mutable.HashMap[Long,Double]()
    while (k < numIteration){

      //println("******** iteration "+k+"********")
      //val a = BroadcastWrapper(sc,w)
      val a = sc.broadcast(w)
      bucket.setCurrentTime(k)

      val pSampled = pointsIndexed.sample(true,b.toFloat/N)
      val te = a

      //TODO: fix set mode
      sc.set_mode(0)
      val SampledIndices = pSampled.map(x=>x._2).collect()

      val sampledMap = new mutable.HashMap[Long,Double]()

      for(j<-SampledIndices){
        if(ScalarMap.contains(j)){

          sampledMap.put(j,ScalarMap.get(j).get)
        }
        else{
          sampledMap.put(j,0)
        }

      }

      val IndexGrad = pSampled.map{x=>

        val e2 = gradfun(x._1,te.value)
        val scal = sampledMap.get(x._2).get
        val oldGrad = scal * new BDV[Double](x._1.features.toArray)

        ((x._2,e2._2),e2._1,oldGrad)
      }

      IndexGrad.ASYNCaggregate(new ListBuffer[(Long,Double)](),BDV.zeros[Double](d),BDV.zeros[Double](d))((x, y)=>{x._1+=y._1;(x._1,x._2+y._2,x._3+y._3)}, (x, y)=>(x._1++y._1,x._2+y._2,x._3+y._3),bucket)

      while(bucket.getSize() <8){
        Thread.sleep(2)
        //println("bucket size is "+ bucket.size())

      }
      for (i<-1 to bucket.getSize()){
        // TODO: add parindex to grad updates (K1, K2, V)
        val data = bucket.getFromBucket()
        val info = data.gettaskResult()
        val lis = info._1
        val gradient = info._2
        val alphai = info._3
        ts = data.getStaleness()
        parRecs = data.getbatchSize()
        parIndex = data.getWorkerID()
        for(key<-lis){
          ScalarMap.put(key._1, key._2)
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
  def gradfun(p:LabeledPoint, w: DenseVector[Double]): (DenseVector[Double],Double) ={
    //println(w)
    val x = new BDV[Double](p.features.toArray)
    val y= p.label
    val scalar = (x.t* w - y)
    val grad = scalar * x
    //alpha(::,index.toInt) := grad

    //println("vec is :"+ w)
    (grad,scalar)
  }

  def gradfunSGD(p:LabeledPoint, w: DenseVector[Double],w2: DenseVector[Double]): DenseVector[Double] ={
    val x = new BDV[Double](p.features.toArray)
    //val hh = h.get(0).get
    val y= p.label
    val grad = (x.t* w - y) * x
    grad
  }







}
