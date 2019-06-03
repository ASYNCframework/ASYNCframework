//scalastyle:off
package org.apache.spark.examples

import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector, Vectors}
import org.apache.spark.{SparkConf, SparkContext, SparkEnv}
import breeze.linalg.{DenseVector => BDV, SparseVector => BSV, Vector => BV}
import breeze.linalg._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.{RDD, ResultsRdd}
import org.apache.spark.rdd.RDDCheckpointData
import spire.random.Dist
import java.io._
import java.util.Random

import BreezeConverters._
import org.apache.spark.examples.BreezeConverters.toBreeze
import org.apache.spark.examples.SparkASGDThread.gradfun
import org.apache.spark.mllib.BLASUtil.{axpyOp, dotOp, scalOp}
import org.apache.spark.util.random.{BernoulliSampler, GapSampling, RandomSampler}

import util.control.Breaks._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
object SparkASAGAThread{

  def main(args: Array[String]): Unit = {

    // inputs from user
    // later check if the file exists
    println("Spark ASAGA application started")
    println("Input arguments:")
    println("Input format: [path name] [file name] [num columns] [num rows]" +
      " [num partitions] [num iterations] [step size] [taw] [batch rate]" +
      " [bucket ratio] [printer freq] [coeff] [seed]")

    if(args.length!= 13){
      println("The input arguments are wrong. ")
      return
    }

    val pathname = args(0)
    val fname = args(1)
    val d = args(2).toInt
    var N = args(3).toInt
    val numPart = args(4).toInt
    val numIteration = args(5).toInt
    val gamma=args(6).toDouble
    val taw = args(7).toInt
    val b= args(8).toDouble
    val bucketRatio = args(9).toDouble
    val printerFreq = args(10).toInt
    val coeff = args(11).toDouble
    val InputSeed = args(12).toLong
    println("path name: " + pathname)
    println("file name: " + fname)
    println("num columns: " + d)
    println("num rows: " + N)
    println("num partitions: " +numPart)
    println("num iterations: " +numIteration)
    println("step size: "+ gamma)
    println("taw: "+taw)
    println("batch rate: "+b)
    println("bucket ratio: "+bucketRatio)
    println("printer freq: " +printerFreq)
    println("coeff: "+coeff)
    println("seed: "+InputSeed)

    var CloudFlag = false
    if (coeff == -1){
      CloudFlag = true
    }

    val conf = new SparkConf().setAppName("ASGAThread.v1")
      .setMaster("local[8]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val points = MLUtils.loadLibSVMFile(sc, pathname+fname).repartition(numPart).sample(false,0.1)
    N = points.count().toInt

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
    val alphaBar= Vectors.zeros(d)
    val w = Vectors.zeros(d)

    //val bucket =new ResultsRdd[DenseVector[Double]]
    val bucket =new ResultsRdd[(ListBuffer[(Long,Double)],Vector)]
    bucket.setRecordStat(false)

    var k = 0
    //var accSize = 0
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
    val optVars = new ListBuffer[(Long,BDV[Double])]
    optVars.append((0,toBreeze(w).toDenseVector))
    //val efficiency = new ListBuffer[(Int,Double)]

    // Anew thread for updating w:
    var xs = 0L
    var xe = 0L

    //NEW:
    val WaitingTime = new mutable.HashMap[Int,Long]()
    val FinishTimeTable = new mutable.HashMap[Int,Long]()

    val SubmitJobTime = new mutable.HashMap[Int,Long]()
    val TaskCompletionTime = new mutable.HashMap[Int,(Int,Long)]()

    //var globalTS= 0
    var accnum = 0
    var extraTimeSt = 0L
    var extraTimeEn = 0L
    var syncTimeFinished =0L

    // This part is for simulating a cloud environment[long tail phenom]
    // first we choose 25% of the partitions to be stragglers
    // then we choose 80% of them to be 1.5x-2.5x slower
    // and we choose the rest to be 2.5x-10x slower
    // note: straggler list contains the indices
    var c =0
    val stragglerNormal = new ListBuffer[Int]
    val stragglerLongTail = new ListBuffer[Int]

    val length = math.round(0.25*numPart).toInt
    val lengthNormal = math.round(0.8*length).toInt
    val lengthLongTail = length-lengthNormal
    for (c<-0 until length){
      if(c<lengthLongTail)
        stragglerLongTail.append(c*4)
      else
        stragglerNormal.append(c*4)
    }
    //println(stragglerLongTail)
    //println(stragglerNormal)
    var culTime = 0.0
    var culCount = 0
    val comOp: (Vector,Vector)=>Vector = (x,y )=>{
      axpyOp(1.0,x,y)
    }
    val startTime = System.currentTimeMillis()
    val thread = new Thread {
      override def run {
        while(k<numIteration){
          val bsize = bucket.getSize()
          //println(bsize)
          if(bsize>0){
            //math.floor(numPart*bucketRatio)
            // empty list of workers to assign for later

            //update the variable
            xs = 0L
            xe = 0L
            extraTimeEn = 0
            extraTimeSt = 0
            for (i<-0 until bsize){
              val info = Option(bucket.getFromBucket())

              info match {
                case Some(value) =>{
                  val data = value.getData()
                  val ts = value.getTimeStamp()

                  if(k-ts <= taw ){
                    val lis = data._1
                    val gradient_alphai = data._2
                    val parIndex = value.getPartitionIndex()
                    FinishTimeTable.put(parIndex,System.currentTimeMillis()-(extraTimeEn-extraTimeSt))
                    if(k<100*numPart){
                      //println(extraTimeEn-extraTimeSt)
                      xs = System.currentTimeMillis()-(extraTimeEn-extraTimeSt)
                      //val taskTime = TaskCompletionTime.get(parIndex).getOrElse((0,0L))
                      //val addedCt = xs - SubmitJobTime.get(parIndex).getOrElse(xs)
                      //TaskCompletionTime.put(parIndex,(taskTime._1 + 1, taskTime._2 +addedCt))
                      culCount += 1
                      culTime += (xs- SubmitJobTime.get(parIndex).getOrElse(xs))
                    }
                    extraTimeSt = System.currentTimeMillis()
                    for(key<-lis){
                      ScalarMap.put(key._1, key._2)
                    }


                    val parRecs = b*N/numPart
                    //println(value.getRecordsNum())
                    //val parRecs = value.getRecordsNum()
                    // adding regularization part
                    // let's don't consider regularization for now

                    val gammaThisIter = -gamma
                    axpyOp(gammaThisIter/parRecs, gradient_alphai, w)
                    axpyOp(gammaThisIter, alphaBar, w)
                    axpyOp(1.0 /N, gradient_alphai, alphaBar)

                    pendingQueue += parIndex

                    if(k%printerFreq ==0){
                      println("Iteration "+ k + " is finished")
                      optVars.append((System.currentTimeMillis()-startTime,toBreeze(w).toDenseVector))
                    }
                    k = k + 1
                    extraTimeEn = System.currentTimeMillis()

                  }else{
                    val parIndex = value.getPartitionIndex()
                    pendingQueue += parIndex
                    accnum+=1
                    if(k%200==0){
                      println("GEEZ"+k +" "+ts)
                    }
                  }


                }
                case None => {
                  throw new NullPointerException
                }
              }
              xe = System.currentTimeMillis()
            }
            //accSize += bsize
            //syncTimeFinished = System.currentTimeMillis()
            //efficiency.append((k,k.toDouble/accSize))
          }else
            Thread.sleep(1)

        }
      }

    }.start()

    var avgDelay = 0.0
    var flag = false
    while (k < numIteration) {
      //println("******** iteration "+k+"********")
      //println(bucket.isOld())

      // if w has not been updated, do nothing(sleep for a bit)
      //println(math.floor(numPart*bucketRatio))
      //println(pendingQueue.size)
      if(pendingQueue.size>=math.floor(numPart*bucketRatio)){
        //define workersList at the beginning of each iteration
        val workersList =  new ListBuffer[Int]()
        while( !pendingQueue.isEmpty ){
          workersList.append(pendingQueue.dequeue())
        }
        //println(workersList)

        //workersList++=pendingList
        //pendingList.clear()

        //println(workersList+ "at "+k)
        val a = sc.broadcast(w)
        val r = new scala.util.Random
        val cTime = System.currentTimeMillis()
        // generate  random sampling on the driver
        // dist is a list containing the sampled
        // indices, it is generated only for workers in the workersList
        //val dist = new ListBuffer[Int]
        var itemIndex = 0
        //val r = new scala.util.Random()
        // generate a map which is sent to workers
        // this maps contains the scalars for computing the historical gradients

        val sampledMap = new mutable.HashMap[Long, Double]()
        for(part<-workersList){
          itemIndex = partitionCumList(part)
          r.setSeed(cTime)
          for (it<-0 until partitonInfo.get(part).get){
            if(r.nextDouble()<b){
              val num = ScalarMap.get(itemIndex).getOrElse(0.0)
              if(num !=0){
                sampledMap.put(itemIndex, num)
              }
            }
            itemIndex+=1
          }
          //println("")
        }


        if(k>100*numPart && !flag){
          flag = true
          //val tInfo = TaskCompletionTime.get(partitionIndex).getOrElse((1,0L))
          //avgDelay = tInfo._2/tInfo._1
          avgDelay = culTime/culCount
        }


        val pFiltered = pointsIndexed.mapPartitionsWithIndex ( (index: Int, it: Iterator[(LabeledPoint,Long)])=>{
          // check what workers have finished their jobs
          // these workers should compute the gradients
          if(workersList.contains(index)){
            if(flag ){
              if(!CloudFlag){
                if(index ==0 && coeff>0){
                  Thread.sleep(math.round(coeff*avgDelay))
                }
              }
              else{
                if (stragglerLongTail.contains(index)){
                  val rnd = new scala.util.Random()
                  val c = rnd.nextDouble()*7.5+2.5
                  Thread.sleep(math.round(c*avgDelay))
                }
                if(stragglerNormal.contains(index)){
                  val rnd = new scala.util.Random()
                  val c = rnd.nextDouble()+1.5
                  Thread.sleep(math.round(c*avgDelay))
                }

              }
            }
            it
          }
          else{
            Iterator.empty
          }

        } )


        // generate random sampling in a distributed way on workers
        val pSampled = pFiltered.mapPartitionsWithIndex((index: Int, iter: Iterator[(LabeledPoint, Long)]) => {
          val r = new scala.util.Random(cTime)
          iter.filter(_ => {
            (r.nextDouble() < b)})
        })
        //val pSampled = pFiltered.sample(false,b,InputSeed+k+1)

        //TODO: fix set mode
        val IndexGrad = pSampled.map { x =>
          val e2 = gradfun(x._1, a.value)
          val scalar = sampledMap.get(x._2).getOrElse(0.0)
          axpyOp(-scalar, x._1.features, e2._1)
          ((x._2, e2._2), e2._1)
        }

        //increase the time stamp by 1
        bucket.setCurrentTime(k)

        IndexGrad.aggregate_async(new ListBuffer[(Long, Double)](), Vectors.zeros(d))((x, y) => {
          x._1 += y._1;
          (x._1, comOp(x._2,y._2))
        }, (x, y) => (x._1 ++ y._1,comOp(x._2,y._2)), bucket)

        //NEW:
        val xe = System.currentTimeMillis()
        for(part<-workersList){
          val delay = WaitingTime.get(part).getOrElse(0L)
          SubmitJobTime.put(part,xe)
          var addedDelay = 0L
          addedDelay= xe - FinishTimeTable.get(part).getOrElse(xe)
          WaitingTime.put(part,delay+addedDelay)
        }

      }
      else{

        //println("bucket is old")
        Thread.sleep(1)
      }
    }






    val stopTime=System.currentTimeMillis()
    val elapsedTime = (stopTime-startTime)
    println("Elapsed time(ms): "+elapsedTime)

    //println("Task Completion Time:")
    //println(TaskCompletionTime)


    //NEW:
    var sum = 0L
    var sumCount = 0
    println("*********************************")
    println("Individual waiting times:")
    for(t<-WaitingTime){
      println(t._1+","+t._2)
      sum += t._2
      sumCount+=1
    }
    println("Average waiting time(ms) per worker and iteration:"+sum/(sumCount*k))



    Thread.sleep(10000)

    // calculate the objective value here
    //val ObjValues = new ListBuffer[Double]()

    //val writer = new PrintWriter(new File(pathname+fname+"-ASAGA-"+coeff+".csv"))
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
    println("*********************************")
    //println("time(ms), objective function value, norm of gradient")
    var obj = 0.0
    var gnorm = 0.0
    //val writer = new PrintWriter(new File(pathname+fname+"-ASAGAThread-"+".csv"))
    //for(wtest<-optVars){
      //obj = points.map(p=> scala.math.pow((new BDV[Double](p.features.toArray).t *wtest._2  - p.label),2)).reduce(_ + _)
      //obj=obj/N
      //+ lambda/2 * scala.math.pow(norm(wtest._2),2)
      //writer.write((wtest._1).toString + ", " + wtest._2.toArray.mkString(",") + "\n" )
      //val gval = points.map(p=>gradfun(p,fromBreeze(wtest._2) )).map(p=>p._1).reduce(comOp)
      //gnorm = math.sqrt(dotOp(gval,gval))/N
      //println(wtest._1+","+obj+","+gnorm)
      //println(wtest._1+","+obj)


    //}
    val obj2 = points.map(p=>{
      var x = BDV.zeros[Double](optVars.length)
      var i = 0
      for(wtest<-optVars){
        x(i) = scala.math.pow((new BDV[Double](p.features.toArray).t *wtest._2  - p.label),2)/(N)
        i=i+1
      }
      x

    }).reduce(_+_)

    var i = 0
    for(wtest<-optVars){
      println(wtest._1+","+obj2(i))
      i=i+1
    }
    //writer.close()
    //println("last objective: "+obj)
    //println(efficiency)
    //println("accnum:"+accnum)
    //println("Elapsed Time: "+elapsedTime)
    //println("avg delay:")
    //println(culTime/culCount)
    println("finished")

  }

  def gradfun(p:LabeledPoint, w: Vector): (Vector,Double) ={

    //val z =toBreeze(p.features)
    //val x = new BDV[Double](p.features.toArray)
    //val x= new BSV[Double](p.features.toArray)
    //val y= p.label
    val grad = Vectors.zeros(w.size)
    val diff = dotOp(p.features, w) - p.label
    axpyOp(diff, p.features, grad)
    //val scalar =(z.t * w - y)
    //System.gc()
    //val grad = z*scalar
    (grad, diff)


  }




}
