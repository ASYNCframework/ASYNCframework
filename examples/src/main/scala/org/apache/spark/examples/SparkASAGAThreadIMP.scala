//scalastyle:off
package org.apache.spark.examples

import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector, Vectors}
import org.apache.spark.{SparkConf, SparkContext, SparkEnv}
import breeze.linalg.{DenseVector => BDV, SparseVector => BSV, Vector => BV}
import breeze.linalg._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.{RDD, ResultsRdd}
import org.apache.spark.rdd.RDDCheckpointData
import spire.random.Dist
import java.io._
import BreezeConverters._

import org.apache.spark.mllib.BLASUtil._
//
// import org.apache.spark.mllib.linalg.BLAS.{axpy, dot, scal}

import util.control.Breaks._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
object SparkASGDThreadIMP{

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
    val InputSeed = args(13).toLong
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
    println("InputSeed: "+InputSeed)


    val conf = new SparkConf().setAppName("Gradient")
      .setMaster("local[8]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    /*val filename = pathname+fname
    var cc = 0
    val writerw = new PrintWriter(new File("epsilon"))

    for (line <- Source.fromFile(filename).getLines) {
      //println(line)
      writerw.write(line+"\n")
      cc= cc+1
      if(cc==10000){
        writerw.close()
      }
    }*/
    val points = MLUtils.loadLibSVMFile(sc, pathname+fname).repartition(numPart)


    // check how balanced the data is:
    val partitonInfo = points.mapPartitionsWithIndex{case (i,rows) => Iterator((i,rows.size))}.reduceByKey((i,j)=>i+j).collectAsMap()
    println(partitonInfo)


    //var alpha = new IndexedRowMatrix()
    val w = Vectors.zeros(d)

    //val bucket =new ResultsRdd[DenseVector[Double]]
    val bucket =new ResultsRdd[Vector]
    bucket.setRecordStat(true)
    var k = 0
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


    val optVars = new ListBuffer[(Long,BDV[Double])]
    //val efficiency = new ListBuffer[(Int,Double)]


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
    //var accJobTime = 0L
    val startTime = System.currentTimeMillis()
    optVars.append((0,toBreeze(w).toDenseVector))
    var avgDelay = 0.0
    var flag = false
    var accnum = 0
    while(k<numIteration){

          if(pendingQueue.size>=math.floor(numPart*bucketRatio)){
            //define workersList at the beginning of each iteration
            val workersList =  new ListBuffer[Int]()
            val Qsize = pendingQueue.size
            for( q<-0 until Qsize){
              workersList.append(pendingQueue.dequeue())
            }

            val a = sc.broadcast(w)

            if(coeff> 0 && k>1600 && !flag){
              flag = true
              val tInfo = TaskCompletionTime.get(0).getOrElse((1,0L))
              avgDelay = tInfo._2/tInfo._1
            }



            val pFiltered = pointsIndexed.mapPartitionsWithIndex ( (index: Int, it: Iterator[(LabeledPoint,Long)])=>{
              // check what workers have finished their jobs
              // these workers should compute the gradients
              if(workersList.contains(index)){
                if(index == 0 && flag){
                  Thread.sleep(math.round(coeff*avgDelay))
                }
                it
              }
              else{
                Iterator()
              }

            } )

            val pSampled  = pFiltered.sample(false,b,InputSeed+k+1)
            //val pSampled  = pointsIndexed.sample(false,b,InputSeed+k+1)

            //val pSampled = pFiltered
            //TODO: fix set mode
            val IndexGrad = pSampled.map { x =>
              gradfun(x._1, a.value)
            }

            bucket.setCurrentTime(k)
            //globalTS+=1
            val comOp: (Vector,Vector)=>Vector = (x,y )=>{
              axpyOp(1.0,x,y)
            }
            IndexGrad.reduce_async(comOp,bucket)

            //println("Submitted2")
            //NEW:
            val xe = System.currentTimeMillis()
            /*for(part<-workersList){
              val delay = WaitingTime.get(part).getOrElse(0L)
              SubmitJobTime.put(part,xe)
              var addedDelay = 0L
              addedDelay= xe - FinishTimeTable.get(part).getOrElse(xe)
              WaitingTime.put(part,delay+addedDelay)
            }*/

          }
          /*else{
            if(k>9000){
              val x =2

            }
          }*/

          val bsize = bucket.getSize()
           //println(bsize+ " k: "+k)
          // if there is anything in the bucket, update
          // otherwise wait
          //println(bsize)
          if(bsize>0){
            for (i<-0 until bsize){
              // TODO: make all info in one data structure
              //var info
              val info = Option(bucket.getFromBucket())

              info match {
                case Some(value) =>{
                  val data = value.getData()
                  val ts = value.getTimeStamp()
                  if(k-ts<=taw ){
                    val gradient = data
                    val parIndex = value.getPartitionIndex()
                    FinishTimeTable.put(parIndex,System.currentTimeMillis())

                    if(k<1000){
                      xs = System.currentTimeMillis()
                      //val jt = xs - SubmitJobTime.get(parIndex).getOrElse(xs)
                      //accJobTime = (accJobTime*(k)+jt)/(k+1)
                      val taskTime = TaskCompletionTime.get(parIndex).getOrElse((0,0L))
                      val addedCt = xs - SubmitJobTime.get(parIndex).getOrElse(xs)
                      TaskCompletionTime.put(parIndex,(taskTime._1 + 1, taskTime._2 +addedCt))
                    }
                    //val trueRec = value.getRecordsNum()
                    val parRecs = b*N/numPart
                    scalOp(1.0 /parRecs, gradient)
                    val gammaThisIter = -gamma / math.sqrt(k / numPart+1)
                    //val gammaThisIter = -gamma
                    axpyOp(gammaThisIter, gradient, w)
                    pendingQueue += parIndex

                    if(k%printerFreq ==0){
                      println("iteration "+ k + " is finished")
                      optVars.append((System.currentTimeMillis()-startTime,toBreeze(w).toDenseVector))
                    }
                    k = k + 1

                  }
                  else{
                    val parIndex = value.getPartitionIndex()
                    pendingQueue += parIndex
                    accnum+=1
                    //println("GEEZ "+" "+globalTS+" "+ts+ " "+k)
                  }


                  //NEW:
                  // record time when partition is freed
                  //println(xe-xs)

                  //add the idle worker to pending list
                  //println("worker: "+ parIndex+ " finished at "+k)
                  // check for the delay based on taw
                  //println("globalTS:"+globalTS+" ts:"+ts)

                }
                case None => {
                  throw new NullPointerException
                }
              }
              //xe = System.currentTimeMillis()
            }

            //accSize += bsize
            syncTimeFinished = System.currentTimeMillis()
            //efficiency.append((k,k.toDouble/accSize))
          }else
            Thread.sleep(1)

        }




    val stopTime=System.currentTimeMillis()
    val elapsedTime = (stopTime-startTime)/1e3
    println("Elapsed Time: "+elapsedTime)
    //println(TaskCompletionTime)

    //NEW:
    println(WaitingTime)
    for(t<-WaitingTime){
      println(t._1+","+t._2)
    }
    //println("accTime: "+acctime/1e3)
    //println("The Final results for obj "+obj+ " is: ")
    //println(w)
    Thread.sleep(2000)

    // calculate the objective value here
    //val ObjValues = new ListBuffer[Double]()

    //val writer = new PrintWriter(new File(pathname+""+System.currentTimeMillis().toString+"_"+fname+"-output.csv"))
    val writer = new PrintWriter(new File(pathname+fname+"-me-1.csv"))

    //val allExecutors = sc.getExecutorMemoryStatus.map(_._1).size
    //writer.write("Number of executors: "+allExecutors+"\n")
    //writer.write("path name: " + pathname+"\n")
    //writer.write("file name: " + fname+"\n")
    //writer.write("num columns: " + d+"\n")
    //writer.write("num rows: " + N+"\n")
    //writer.write("num partitions: " +numPart+"\n")
    //writer.write("num iterations: " +numIteration+"\n")
    //writer.write("Step Size: "+ gamma+"\n")
    //writer.write("taw: "+taw+"\n")
    //writer.write("batch Size: "+b+"\n")
    //writer.write("bucket ratio: "+bucketRatio+"\n")
    //writer.write("printer freq: " +printerFreq+"\n")
    //writer.write("checking time: "+sleepTime+"\n")
    //writer.write("lambda: "+lambda+"\n")
    //writer.write("coeff: "+coeff+"\n")
    sc.set_mode(0)
    //println(points.count())
    //points.foreach(x=>println(x.label))
    for(wtest<-optVars){

      //obj = points.map(p=> scala.math.pow((new BDV[Double](p.features.toArray).t *wtest._2  - p.label),2)).reduce(_ + _)
      //obj=obj/N
      //+ lambda/2 * scala.math.pow(norm(wtest._2),2)
      writer.write((wtest._1).toString + ", " + wtest._2.toArray.mkString(",") + "\n" )
      //println("objective function : "+obj)

    }
    writer.close()
    println("accnum: "+accnum)
    //println("last objective: "+obj)
    //println("average delay:"+avgDelay)
    //println(efficiency)

  }
  /*def toBreeze(v: Vector): BV[Double] = v match {
    case DenseVector(values) => new BDV[Double](values)
    case SparseVector(size, indices, values) => {
      new BSV[Double](indices, values, size)
    }
  }

  def fromBreeze(v: BV[Double]) :Vector = v match {
    case
  }*/
  def gradfun(p:LabeledPoint, w: Vector): (Vector) ={

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
    grad


  }

  def objective(p:LabeledPoint, w: BDV[Double]): BDV[Double] ={
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

