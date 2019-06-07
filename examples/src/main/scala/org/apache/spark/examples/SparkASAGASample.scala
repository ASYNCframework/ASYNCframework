//scalastyle:off
package org.apache.spark.examples

import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext, SparkEnv}
import breeze.linalg.{DenseVector => BDV, SparseVector => BSV, Vector => BV}
import breeze.linalg._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.{RDD, ASYNCcontext}
import org.apache.spark.rdd.RDDCheckpointData
import spire.random.Dist
import java.io._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
object SparkASAGASample{

  def main(args: Array[String]): Unit = {

    // inputs from user
    // later check if the file exists
    println("Input arguments:")
    println("input format: [path name] [file name] [num columns] [num rows] [num partitions] [num iterations] [Step Size] [taw] [batch Size] [bucket limit<=num partitions] [printer freq] [checking time] [lambda]")

    val pathname = args(0)
    val fname = args(1)
    val d = args(2).toInt
    val N = args(3).toInt
    val numPart = args(4).toInt
    val numIteration = args(5).toInt
    val gamma=args(6).toDouble
    val taw = args(7).toInt
    val b= args(8).toInt
    val bucketRatio = args(9).toDouble
    val printerFreq = args(10).toInt
    val sleepTime = args(11).toInt
    val lambda = args(12).toDouble

    println("path name: " + pathname)
    println("file name: " + fname)
    println("num columns: " + d)
    println("num rows: " + N)
    println("num partitions: " +numPart)
    println("num iterations: " +numIteration)
    println("Step Size: "+ gamma)
    println("taw: "+taw)
    println("batch Size: "+b)
    println("bucket ratio: "+bucketRatio)
    println("printer freq: " +printerFreq)
    println("checking time: "+sleepTime)
    println("lambda: "+lambda)


    val conf = new SparkConf().setAppName("Gradient").setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val points = MLUtils.loadLibSVMFile(sc, pathname+fname).repartition(numPart)


    // check how balanced the data is:
    val partitonInfo = points.mapPartitionsWithIndex{case (i,rows) => Iterator((i,rows.size))}.reduceByKey((i,j)=>i+j).collectAsMap()

    // starting index for each partition
    val partitionCumNum = new ListBuffer[Int]
    var cumNum = 0
    partitionCumNum.append(cumNum)
    for(part<-0 until numPart){
      cumNum += partitonInfo.get(part).get
      partitionCumNum.append(cumNum)
    }
    val partitionCumList = partitionCumNum.toList
    println(partitionCumList)


    //var alpha = new IndexedRowMatrix()
    var alphaBar= BDV.zeros[Double](d)
    var w = BDV.zeros[Double](d)

    //val bucket =new ResultsRdd[DenseVector[Double]]
    val bucket =new ASYNCcontext[(ListBuffer[(Long,Double)],DenseVector[Double],DenseVector[Double])]

    var k = 0
    var ts = 0
    var parRecs = 0
    var parIndex = 0
    var obj = 0.0
    var accSize = 0
    val pointsIndexed = points.zipWithIndex.cache()
    //val p2 = pointsIndexed.mapPartitionsWithIndex{case (i,rows) => rows.toList.map(x=>i+" "+ x._2).iterator}




    val ScalarMap= new mutable.HashMap[Long,Double]()
    val optVars = new ListBuffer[(Long,DenseVector[Double])]
    val efficiency = new ListBuffer[(Int,Double)]
    val startTime = System.currentTimeMillis()
    optVars.append((0,w))
    var accTimeRand = 0L

    // define another workersList to solve the following problem
    // when we change workersList before broadcasting it
    // the previous Tasks might get another copy of that which is not
    // correct for that iteration
    val pendingList = new ListBuffer[Int]
    for(part<-0 until numPart){
      pendingList.append(part)
    }
    while (k < numIteration){

      //println("******** iteration "+k+"********")

      //define workersList at the beginning of each iteration
      val workersList =  new ListBuffer[Int]()
      workersList++=pendingList

      val a = sc.broadcast(w)
      bucket.setCurrentTime(k)
      val r = new scala.util.Random
      val cTime = System.currentTimeMillis()

      // generate  random sampling on the driver:

      // dist is a list containing the sampled
      // indices, it is generated only for workers in the workersList
      val dist = new ListBuffer[Int]
      var itemIndex = 0

      for(part<-workersList){
        itemIndex = partitionCumList(part)
        r.setSeed(cTime)
        for (it<-0 until partitonInfo.get(part).get){
          if(r.nextDouble()<b.toDouble/N){
            dist.append(itemIndex)
          }
          itemIndex+=1
        }
      }




      val pFiltered = pointsIndexed.mapPartitionsWithIndex ( (index: Int, it: Iterator[(LabeledPoint,Long)])=>{
        // check what workers have finished their jobs
        // these workers should compute the gradients

        if(workersList.contains(index)){
          it
        }
        else{
          Iterator()
        }

      } )

      // generate random sampling in a distributed way on workers
      val pSampled = pFiltered.mapPartitionsWithIndex((index:Int,iter:Iterator[(LabeledPoint,Long)])=>{
        val r = new scala.util.Random(cTime)
        iter.filter(_=>{
          if(r.nextDouble()<b.toDouble/N){
            true
          }else{
            false
          }
        })
      })


      //TODO: fix set mode

      // generate a map which is sent to workers
      // this maps contains the scalars for computing the historical gradients
      val sampledMap = new mutable.HashMap[Long,Double]()

      for(j<-dist){
        if(ScalarMap.contains(j)){
          sampledMap.put(j,ScalarMap.get(j).get)
        }
        else{
          sampledMap.put(j,0)
        }

      }


      val IndexGrad = pSampled.map{x=>
        val e2 = gradfun(x._1,a.value)
        val scalar = sampledMap.get(x._2).get
        val oldGrad = scalar * new BDV[Double](x._1.features.toArray)

        ((x._2,e2._2),e2._1,oldGrad)
      }

      IndexGrad.ASYNCaggregate(new ListBuffer[(Long,Double)](),BDV.zeros[Double](d),BDV.zeros[Double](d))((x, y)=>{x._1+=y._1;(x._1,x._2+y._2,x._3+y._3)}, (x, y)=>(x._1++y._1,x._2+y._2,x._3+y._3),bucket)

      // wait until part of the workers are finished
      var bsize = bucket.getSize()
      while(bsize < math.floor(numPart*bucketRatio) ){
        Thread.sleep(sleepTime)
        bsize = bucket.getSize()

      }

      pendingList.clear()
      //workersList.clear()
      val stTimeRand = System.currentTimeMillis()

      //update the variable
      for (i<-1 to bsize){
        //Thread.sleep(10)
        // TODO: add parindex to grad updates (K1, K2, V)
        val data = bucket.getFromBucket()
        val info = data.gettaskResult()
        val lis = info._1
        var gradient = info._2
        val alphai = info._3
        ts = data.getStaleness()
        parRecs = data.getbatchSize()
        parIndex = data.getWorkerID()
        pendingList.append(parIndex)
        //workersList.append(parIndex)
        // check for the delay based on taw
        if(k-ts<taw ){


          for(key<-lis){
            ScalarMap.put(key._1, key._2)
          }

          alphaBar :*= N.toDouble
          alphaBar = ( alphaBar -  alphai + gradient)
          alphaBar :*= (1.0/N)

          alphai :*= (2.0 /parRecs)
          gradient :*= (2.0 /parRecs)

          // adding regularization part
          gradient = gradient + lambda * w

          w = w - gamma * (gradient -alphai +alphaBar)
          if(k%printerFreq ==0){
            println("iteration "+ k + " is finished")
            optVars.append((System.currentTimeMillis()-startTime,w))
          }
          k = k + 1
          //println(k)
        }



      }
      val endTimeRand = System.currentTimeMillis()
      accTimeRand = accTimeRand + endTimeRand-stTimeRand

      accSize += bsize
      //efficiency.append((k,k.toDouble/accSize))

      /*if(k%10 == 0){
        //te.setID(latestIndex)
        sc.set_mode(0)
        obj = pointschr.map(p=> scala.math.pow((new BDV[Double](p.features.toArray).t *w  - p.label),2)).reduce(_ + _)
        obj=obj/N
        println("objective function : "+obj+" after "+ k+ " iterations")

      }*/

    }

    val stopTime=System.currentTimeMillis()
    val elapsedTime = (stopTime-startTime)/1e3
    println("Elapsed Time: "+elapsedTime)
    println("Randomization Time: "+accTimeRand/1e3)
    //println("The Final results for obj "+obj+ " is: ")
    //println(w)
    Thread.sleep(10000)

    // calculate the objective value here
    //val ObjValues = new ListBuffer[Double]()

    val writer = new PrintWriter(new File(pathname+""+System.currentTimeMillis().toString+"_"+fname+"-output.csv"))

    sc.set_mode(0)
    for(wtest<-optVars){
      obj = points.map(p=> scala.math.pow((new BDV[Double](p.features.toArray).t *wtest._2  - p.label),2)).reduce(_ + _)
      obj=obj/N + lambda/2 * scala.math.pow(norm(wtest._2),2)
      writer.write((wtest._1).toString + ", " + obj.toString + "\n" )
      //println("objective function : "+obj)

    }
    writer.close()
    println("last objective: "+obj)
    //println(efficiency)

  }
  def gradfun(p:LabeledPoint, w: DenseVector[Double]): (DenseVector[Double],Double) ={
    //println(w)
    val a = p.features.toArray
    val x = new BDV[Double](a)
    //val x= new BSV[Double](p.features.toArray)
    val y= p.label
    val scalar = (x.t* w - y)
    val grad = scalar * x
    //alpha(::,index.toInt) := grad

    //println("vec is :"+ w)
    (grad,scalar)
  }

  def objective(p:LabeledPoint, w: DenseVector[Double]): DenseVector[Double] ={
    //println(w)
    val x = new BDV[Double](p.features.toArray)
    val y= p.label
    val scalar = (x.t* w - y)
    val grad = scalar * x
    //alpha(::,index.toInt) := grad

    //println("vec is :"+ w)
    grad
  }







}
