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

import org.apache.spark.examples.BreezeConverters.{fromBreeze, toBreeze}
import org.apache.spark.mllib.BLASUtil.{axpyOp, dotOp, scalOp}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.util.random.BernoulliSampler

import util.control.Breaks._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
object SparkASAGASync{

  def main(args: Array[String]): Unit = {

    // inputs from user
    // later check if the file exists
    println("Spark SAGA application started")

    println("Input arguments:")
    println("Input format: [path name] [file name] [num columns] [num rows]" +
      " [num partitions] [num iterations] [step size] [batch rate] " +
      " [printer freq] [coeff] [seed]")

    if(args.length!= 11){
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
    val b= args(7).toDouble
    val printerFreq = args(8).toInt
    val coeff = args(9).toDouble
    val InputSeed = args(10).toLong
    println("path name: " + pathname)
    println("file name: " + fname)
    println("num columns: " + d)
    println("num rows: " + N)
    println("num partitions: " +numPart)
    println("num iterations: " +numIteration)
    println("step size: "+ gamma)
    println("batch rate: "+b)
    println("printer freq: " +printerFreq)
    println("coeff: "+coeff)
    println("seed: "+InputSeed)

    var CloudFlag = false
    if (coeff == -1){
      CloudFlag = true
    }


    val conf = new SparkConf().setAppName("ASGASync.v1")
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
    var avgDelay = 0.0
    var flag = false

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

    while(k<numIteration){

          //println("******** iteration "+k+"********")
          //println(bucket.isOld())

          // if w has not been updated, do nothing(sleep for a bit)
            val a = sc.broadcast(w)
            val cTime = System.currentTimeMillis()
            // generate  random sampling on the driver
            // dist is a list containing the sampled
            // indices, it is generated only for workers in the workersList
            //val dist = new ListBuffer[Int]
            var itemIndex = 0
            //captureSt =System.currentTimeMillis()
            val sampledMap = new mutable.HashMap[Long, Double]()

            for(part<-0 until numPart){
              itemIndex = partitionCumList(part)
              val r = new scala.util.Random(cTime)

              for (it<-0 until partitonInfo.get(part).get){
                if(r.nextDouble()<b){
                  val num = ScalarMap.get(itemIndex).getOrElse(0.0)
                  if(num !=0){
                    sampledMap.put(itemIndex, num)
                  }
                }
                itemIndex+=1
              }
            }

            if( k>100 && !flag){
              flag = true
              //val tInfo = TaskCompletionTime.get(0).getOrElse((1,0L))
              //avgDelay = tInfo._2/tInfo._1
              avgDelay = culTime/culCount

            }

            val pFiltered = pointsIndexed.mapPartitionsWithIndex ( (index: Int, it: Iterator[(LabeledPoint,Long)])=>{
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
            for(part<-0 until numPart){
              val delay = WaitingTime.get(part).getOrElse(0L)
              SubmitJobTime.put(part,xe)
              var addedDelay = 0L
              addedDelay= xe - FinishTimeTable.get(part).getOrElse(xe)
              WaitingTime.put(part,delay+addedDelay)
            }


          var accGrad = BDV.zeros[Double](d)
          var bsize = 0
          var extraTimeSt = 0L
          var extraTimeEn = 0L
          while( bsize<numPart) {
            //math.floor(numPart*bucketRatio)
            // empty list of workers to assign for later
            val info = Option(bucket.getFromBucket())
            info match {
              case Some(value) => {
                bsize += 1
                val parIndex = value.getPartitionIndex()
                FinishTimeTable.put(parIndex, System.currentTimeMillis()-(extraTimeEn-extraTimeSt))

                if (k < 100) {
                  //println(extraTimeEn-extraTimeSt)
                  xs = System.currentTimeMillis() - ((extraTimeEn-extraTimeSt))
                  //val taskTime = TaskCompletionTime.get(parIndex).getOrElse((0, 0L))
                  //val addedCt = xs - SubmitJobTime.get(parIndex).getOrElse(xs)
                  //TaskCompletionTime.put(parIndex, (taskTime._1 + 1, taskTime._2 + addedCt))
                  culCount += 1
                  culTime += (xs-SubmitJobTime.get(parIndex).getOrElse(xs))
                }
                extraTimeSt = System.currentTimeMillis()
                val data = value.getData()
                accGrad += toBreeze(data._2)
                val lis = data._1
                //val gradient_alphai = data._2
                for (key <- lis) {
                  ScalarMap.put(key._1, key._2)
                }
                extraTimeEn = System.currentTimeMillis()

              }
              case None => {
                throw new NullPointerException
              }
            }
          }
          // adding regularization part
          // let's don't consider regularization for now
          val parRecs = b*N
          val gammaThisIter = -gamma
          axpyOp(gammaThisIter/parRecs, fromBreeze(accGrad), w)
          axpyOp(gammaThisIter, alphaBar, w)
          axpyOp(1.0 /N, fromBreeze(accGrad), alphaBar)


          if(k%printerFreq ==0){
            println("Iteration "+ k + " is finished")
            optVars.append((System.currentTimeMillis()-startTime,toBreeze(w).toDenseVector))
          }
          k = k + 1


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
    println("Average waiting time(ms) per worker and iteration:"+sum/(sumCount*k*numPart))


    //println("accTime: "+acctime/1e3)
    //println("The Final results for obj "+obj+ " is: ")
    //println(w)
    Thread.sleep(10000)

    // calculate the objective value here
    //val ObjValues = new ListBuffer[Double]()

    //val writer = new PrintWriter(new File(pathname+fname+"-ASAGA-sync-"+coeff+".csv"))
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
    //val writer = new PrintWriter(new File(pathname+fname+"-ASAGASync-"+".csv"))

    //for(wtest<-optVars){
      //obj = points.map(p=> scala.math.pow((new BDV[Double](p.features.toArray).t *wtest._2  - p.label),2)).reduce(_ + _)
      //obj=obj/N
      //writer.write((wtest._1).toString + ", " + wtest._2.toArray.mkString(",") + "\n" )
      //val gval = points.map(p=>gradfun(p,wtest._2)).map(p=>p._1).reduce(comOp)
      //gnorm = math.sqrt(dotOp(gval,gval))/N
      //+ lambda/2 * scala.math.pow(norm(wtest._2),2)
      //writer.write((wtest._1).toString + ", " + wtest._2.toArray.mkString(",") + "\n" )
      //println("objective function : "+obj)
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
