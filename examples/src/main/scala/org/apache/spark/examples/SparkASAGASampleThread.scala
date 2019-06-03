//scalastyle:off
package org.apache.spark.examples

import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext, SparkEnv}
import breeze.linalg.{DenseVector => BDV, SparseVector => BSV, Vector => BV}
import breeze.linalg._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.{RDD, ResultsRdd}
import org.apache.spark.rdd.RDDCheckpointData
import spire.random.Dist
import java.io._
import util.control.Breaks._


import scala.collection.mutable
import scala.collection.mutable.ListBuffer
object SparkASAGASampleThread{

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
    val b= args(8).toDouble
    val bucketRatio = args(9).toDouble
    val printerFreq = args(10).toInt
    val sleepTime = args(11).toInt
    //val lambda = args(12).toDouble
    val coeff = args(12).toDouble

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
    //println("lambda: "+lambda)
    println("coeff: "+coeff)



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
    val bucket =new ResultsRdd[(ListBuffer[(Long,Double)],DenseVector[Double])]

    var k = 0
    var ts = 0
    var parRecs = 0
    var parIndex = 0
    var obj = 0.0
    var accSize = 0
    val pointsIndexed = points.zipWithIndex.cache()
    //val p2 = pointsIndexed.mapPartitionsWithIndex{case (i,rows) => rows.toList.map(x=>i+" "+ x._2).iterator}

    // define another workersList to solve the following problem
    // when we change workersList before broadcasting it
    // the previous Tasks might get another copy of that which is not
    // correct for that iteration
    val pendingQueue = new mutable.Queue[Int]()
    for(part<-0 until numPart){
      pendingQueue += part
    }



    val ScalarMap= new mutable.HashMap[Long,Double]()
    val optVars = new ListBuffer[(Long,DenseVector[Double])]
    //val efficiency = new ListBuffer[(Int,Double)]
    val startTime = System.currentTimeMillis()
    optVars.append((0,w))

    var Qsize = pendingQueue.size
    // Anew thread for updating w:
    var xs = 0L
    var xe = 0L

    //NEW:
    val WaitingTime = new mutable.HashMap[Int,Long]()
    val FinishTimeTable = new mutable.HashMap[Int,Long]()

    val SubmitJobTime = new mutable.HashMap[Int,Long]()
    val TaskCompletionTime = new mutable.HashMap[Int,(Int,Long)]()

    var globalTS= 0
    var syncTimeFinished =0L
    val thread = new Thread {
      override def run {
        while(k<numIteration){
          //bucket.ResultList.foreach()
          val bsize = bucket.getSize()
          //println(bsize)
          // if there is anything in the bucket, update
          // otherwise wait
          //println(bsize)
          if(bsize>0){
            //math.floor(numPart*bucketRatio)
            // empty list of workers to assign for later

            //update the variable
            xs = 0L
            xe = 0L
            for (i<-0 until bsize){
              // TODO: make all info in one data structure
              //var info
              val info = Option(bucket.getFromBucket())

                info match {
                  case Some(value) =>{
                    val data = value.getData()
                    val lis = data._1

                    //var gradient = data._2
                    //val alphai = data._3
                    var gradient_alphai = data._2

                    val parIndex = value.getPartitionIndex()
                    val parRecs = value.getRecordsNum()
                    val ts = value.getTimeStamp()

                    //NEW:
                    // record time when partition is freed
                    //println(xe-xs)
                    FinishTimeTable.put(parIndex,System.currentTimeMillis()-(xe-xs))
                    xs = System.currentTimeMillis()
                    if(k<1000){
                      val taskTime = TaskCompletionTime.get(parIndex).getOrElse((0,0L))
                      val addedCt = xs - SubmitJobTime.get(parIndex).getOrElse(xs)

                      TaskCompletionTime.put(parIndex,(taskTime._1 + 1, taskTime._2 +addedCt))
                    }
                    //add the idle worker to pending list
                    //println("worker: "+ parIndex+ " finished at "+k)
                    pendingQueue += parIndex
                    // check for the delay based on taw
                    if(globalTS-ts<taw ){


                      for(key<-lis){
                        ScalarMap.put(key._1, key._2)
                      }
                      //Thread.sleep(10)
                      alphaBar :*= N.toDouble
                      //alphaBar = ( alphaBar + gradient-  alphai )
                      alphaBar = ( alphaBar + gradient_alphai )

                      alphaBar :*= (1.0/N)

                      //alphai :*= (2.0 /parRecs)
                      //gradient :*= (2.0 /parRecs)

                      gradient_alphai :*= (2.0 /parRecs)
                      // adding regularization part
                      // let's don't consider regularization for now
                      //gradient = gradient + lambda * w

                      //w = w - gamma * (gradient -alphai +alphaBar)
                      w = w - gamma * (gradient_alphai +alphaBar)

                      if(k%printerFreq ==0){
                        println("iteration "+ k + " is finished")
                        optVars.append((System.currentTimeMillis()-startTime,w))
                      }
                      k = k + 1
                      //bucket.setCurrentTime(k)
                      //println("bucket updated")
                    }
                    else{
                      println("GEEZ")
                    }
                  }
                  case None => {
                    throw new NullPointerException
                  }
                }
              xe = System.currentTimeMillis()
            }

            accSize += bsize
            syncTimeFinished = System.currentTimeMillis()
            //efficiency.append((k,k.toDouble/accSize))
          }else
            Thread.sleep(1)

        }
      }

    }.start()


    while (k < numIteration) {
      //println("******** iteration "+k+"********")
      //println(bucket.isOld())

      // if w has not been updated, do nothing(sleep for a bit)
      if(pendingQueue.size>math.floor(numPart*bucketRatio)){
        //define workersList at the beginning of each iteration
        val workersList =  new ListBuffer[Int]()
        val Qsize = pendingQueue.size
        for( q<-0 until Qsize){
          workersList.append(pendingQueue.dequeue())
        }

        //workersList++=pendingList
        //pendingList.clear()

        //println(workersList+ "at "+k)
        val a = sc.broadcast(w)
        val r = new scala.util.Random
        val cTime = System.currentTimeMillis()

        // generate  random sampling on the driver
        // dist is a list containing the sampled
        // indices, it is generated only for workers in the workersList
        val dist = new ListBuffer[Int]
        var itemIndex = 0

        for(part<-workersList){
          itemIndex = partitionCumList(part)
          r.setSeed(cTime)
          for (it<-0 until partitonInfo.get(part).get){
            if(r.nextDouble()<b){
              dist.append(itemIndex)
            }
            itemIndex+=1
          }
        }

        val tInfo = TaskCompletionTime.get(0).getOrElse((1,0L))
        val avgDelay = tInfo._2/tInfo._1

        val pFiltered = pointsIndexed.mapPartitionsWithIndex ( (index: Int, it: Iterator[(LabeledPoint,Long)])=>{
          // check what workers have finished their jobs
          // these workers should compute the gradients
          //println(math.round(coeff*avgDelay))
          //println(avgDelay)
          if(workersList.contains(index)){
            if(index == 0){
              Thread.sleep(math.round(coeff*avgDelay))
            }
            it
          }
          else{
            Iterator()
          }

        } )

        // generate random sampling in a distributed way on workers
        val pSampled = pFiltered.mapPartitionsWithIndex((index: Int, iter: Iterator[(LabeledPoint, Long)]) => {
          val r = new scala.util.Random(cTime)
          iter.filter(_ => {
            (r.nextDouble() < b)})
        })

        //TODO: fix set mode

        // generate a map which is sent to workers
        // this maps contains the scalars for computing the historical gradients
        val sampledMap = new mutable.HashMap[Long, Double]()

        for (j <- dist) {
          if (ScalarMap.contains(j)) {
            var num = 0.0
            try{
              num = ScalarMap.get(j).get
            }catch{
              case e: Exception=> num = 0.0
            }
            sampledMap.put(j,num)
          }
          /*else {
            sampledMap.put(j, 0)
          }*/

        }


        val IndexGrad = pSampled.map { x =>
          val e2 = gradfun(x._1, a.value)
          val scalar = sampledMap.get(x._2).getOrElse(0.0)
          val oldGrad = scalar * new BDV[Double](x._1.features.toArray)
          //((x._2, e2._2), e2._1, oldGrad)
          ((x._2, e2._2), e2._1 - oldGrad)
        }

        //increase the time stamp by 1
        bucket.setCurrentTime(globalTS)
        globalTS+=1
        IndexGrad.aggregate_async(new ListBuffer[(Long, Double)](), BDV.zeros[Double](d))((x, y) => {
          x._1 += y._1;
          (x._1, x._2 + y._2)
        }, (x, y) => (x._1 ++ y._1, x._2 + y._2), bucket)

        //NEW:
        val xe = System.currentTimeMillis()
        for(part<-workersList){
          val delay = WaitingTime.get(part).getOrElse(0L)
          SubmitJobTime.put(part,xe)
          var addedDelay = 0L
          addedDelay= xe - FinishTimeTable.get(part).getOrElse(xe)
          /*if(bucketRatio>0.9){
            if(syncTimeFinished>0){
              addedDelay = xe - syncTimeFinished

            }
          } else{
            addedDelay= xe - FinishTimeTable.get(part).getOrElse(xe)

          }*/
          WaitingTime.put(part,delay+addedDelay)
        }

      }
      else{

        //println("bucket is old")
        Thread.sleep(1)
      }
    }






    val stopTime=System.currentTimeMillis()
    val elapsedTime = (stopTime-startTime)/1e3
    println("Elapsed Time: "+elapsedTime)
    println("Task Completion Time:")
    println(TaskCompletionTime)

    //NEW:
    println("Waiting Time:")
    println(WaitingTime)
    for(t<-WaitingTime){
      println(t._1+","+t._2)
    }
    //println("accTime: "+acctime/1e3)
    //println("The Final results for obj "+obj+ " is: ")
    //println(w)
    Thread.sleep(10000)

    // calculate the objective value here
    //val ObjValues = new ListBuffer[Double]()

    val writer = new PrintWriter(new File(pathname+fname+"-ASAGA-"+coeff+".csv"))
    //val allExecutors = sc.getExecutorMemoryStatus.map(_._1).size
    /*writer.write("Number of executors: "+allExecutors+"\n")
    writer.write("path name: " + pathname+"\n")
    writer.write("file name: " + fname+"\n")
    writer.write("num columns: " + d+"\n")
    writer.write("num rows: " + N+"\n")
    writer.write("num partitions: " +numPart+"\n")
    writer.write("num iterations: " +numIteration+"\n")
    writer.write("Step Size: "+ gamma+"\n")
    writer.write("taw: "+taw+"\n")
    writer.write("batch Size: "+b+"\n")
    writer.write("bucket ratio: "+bucketRatio+"\n")
    writer.write("printer freq: " +printerFreq+"\n")
    writer.write("checking time: "+sleepTime+"\n")
    writer.write("coeff: "+coeff+"\n")*/

    sc.set_mode(0)
    for(wtest<-optVars){
      //obj = points.map(p=> scala.math.pow((new BDV[Double](p.features.toArray).t *wtest._2  - p.label),2)).reduce(_ + _)
      //obj=obj/N
      //+ lambda/2 * scala.math.pow(norm(wtest._2),2)
      writer.write((wtest._1).toString + ", " + wtest._2.toArray.mkString(",") + "\n" )
      //println("objective function : "+obj)

    }
    writer.close()
    //println("last objective: "+obj)
    //println(efficiency)
    println("finished")

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
