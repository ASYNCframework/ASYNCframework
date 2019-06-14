//scalastyle:off

package org.apache.async

import breeze.linalg.{DenseVector => BDV}
import breeze.linalg._
import org.apache.async.BreezeConverters._
import org.apache.spark.mllib.BLASUtil._
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.{ASYNCcontext, workerState}
import org.apache.spark.{SparkConf, SparkContext}
//
// import org.apache.spark.mllib.linalg.BLAS.{axpy, dot, scal}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object SparkASGDThread{

  def main(args: Array[String]): Unit = {

    // inputs from user
    // later check if the file exists
    println("Spark ASGD application started")
    println("Input arguments:")
    println("Input format: [path name] [file name] [num columns] [num rows]" +
      " [num partitions] [num iterations] [step size] [taw] [batch Rate]" +
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
    val conf = new SparkConf().setAppName("ASGDThread.v1")
    .setMaster("local[8]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val points = MLUtils.loadLibSVMFile(sc, pathname+fname).repartition(numPart) //.sample(false,0.1)
    N = points.count().toInt

    // check how balanced the data is:
    val partitonInfo = points.mapPartitionsWithIndex{case (i,rows) => Iterator((i,rows.size))}.reduceByKey((i,j)=>i+j).collectAsMap()
    println(partitonInfo)


    //var alpha = new IndexedRowMatrix()
    val w = Vectors.zeros(d)

    //val bucket =new ResultsRdd[DenseVector[Double]]
    val AC =new ASYNCcontext[Vector]()
    AC.setRecordStat(false)
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
    //val TaskCompletionTime = new mutable.HashMap[Int,(Int,Long)]()

    var syncTimeFinished =0L
    //var accJobTime = 0L

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

    //var f = (x: STATstruct) => x.state.foreach(y=>y.)

    var culTime = 0.0
    var culCount = 0

    val comOp: (Vector,Vector)=>Vector = (x,y )=>{
      axpyOp(1.0,x,y)
    }

    val startTime = System.currentTimeMillis()
    val thread = new Thread {
      override def run {
        while(k<numIteration){

          val bsize = AC.getSize()
          if(bsize>0){
            xs = 0L
            xe = 0L
            for (i<-0 until bsize){
              val info = Option(AC.ASYNCcollectAll())

              info match {
                case Some(value) =>{
                  val data = value.gettaskResult()
                  val staleness = value.getStaleness()
                  //println("stalenes:"+staleness)
                  //println("max staleness:"+AC.STAT.get(0).get.getMaxStaleness())
                  //println("available workers:"+AC.STAT.get(0).get.getAvailableWorkers())

                  if(staleness<=taw ){
                    val gradient = data
                    val parIndex = value.getWorkerID()
                    FinishTimeTable.put(parIndex,System.currentTimeMillis())

                    if(k<100*numPart){
                      xs = System.currentTimeMillis()
                      //val jt = xs - SubmitJobTime.get(parIndex).getOrElse(xs)
                      //accJobTime = (accJobTime*(k)+jt)/(k+1)
                      //val taskTime = TaskCompletionTime.get(parIndex).getOrElse((0,0L))
                      //val addedCt = xs - SubmitJobTime.get(parIndex).getOrElse(xs)
                      //TaskCompletionTime.put(parIndex,(taskTime._1 + 1, taskTime._2 +addedCt))
                      culCount += 1
                      culTime += (xs- SubmitJobTime.get(parIndex).getOrElse(xs))
                    }
                    //val trueRec = value.getRecordsNum()
                    val parRecs = b*N/numPart
                    scalOp(1.0 /parRecs, gradient)
                    val gammaThisIter = -gamma / math.sqrt(k / numPart+1)
                    //val gammaThisIter = -gamma
                    axpyOp(gammaThisIter, gradient, w)
                    pendingQueue += parIndex

                    if(k%printerFreq ==0){
                      println("Iteration "+ k + " is finished")
                      optVars.append((System.currentTimeMillis()-startTime,toBreeze(w).toDenseVector))
                    }
                    k = k + 1

                  }
                  else{
                    val parIndex = value.getWorkerID()
                    pendingQueue += parIndex
                  }



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
      }

    }.start()

    var avgDelay = 0.0
    var flag = false
    while (k < numIteration) {
      //println("******** iteration "+k+"********")
      //if(pendingQueue.size>=math.floor(numPart*bucketRatio)){
      var init_workers= numPart
      if(k!=0){
        init_workers = AC.STAT.get(0).get.getAvailableWorkers()
      }
      if(init_workers>=math.floor(numPart*bucketRatio)){
        //define workersList at the beginning of each iteration
        val workersList =  new ListBuffer[Int]()
        val Qsize = pendingQueue.size
        for( q<-0 until Qsize){
          workersList.append(pendingQueue.dequeue())
        }

        val a = sc.broadcast(w)
        //val a =AC.ASYNCbroadcast(w)
        if(k>100*numPart && !flag){
          flag = true
          //val tInfo = TaskCompletionTime.get(0).getOrElse((1,0L))
          //avgDelay = tInfo._2/tInfo._1
          avgDelay = culTime/culCount
        }

        //println(avgDelay)

        /*val pFiltered = pointsIndexed.mapPartitionsWithIndex ( (index: Int, it: Iterator[(LabeledPoint,Long)])=>{
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

        } )*/
        val f = (state: workerState[Vector])=>state.getAvailableWorkers()>= math.floor(numPart*bucketRatio)
        val pFiltered1=pointsIndexed.ASYNCbarrier(f,AC.STAT)
        val pFiltered = pFiltered1.mapPartitionsWithIndex ( (index: Int, it: Iterator[(LabeledPoint,Long)])=>{
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

        val pSampled  = pFiltered.sample(false,b,InputSeed+k+1)
        //val pSampled  = pointsIndexed.sample(false,b,InputSeed+k+1)

        //val pSampled = pFiltered
        //TODO: fix set mode
        val IndexGrad = pSampled.map { x =>
          gradfun(x._1, a.value)
        }

       // bucket.setCurrentTime(k)
        //globalTS+=1


        IndexGrad.ASYNCreduce(comOp,AC)

        //println("Submitted2")
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

    Thread.sleep(1000)

    sc.set_mode(0)
    //println(points.count())
    //points.foreach(x=>println(x.label))
    println("*********************************")
    //println("time(ms), objective function value, norm of gradient")
    var obj = 0.0
    var gnorm = 0.0
    //val writer = new PrintWriter(new File(pathname+fname+"-ASGDThread-"+".csv"))
    /*for(wtest<-optVars){

      obj = points.map(p=> scala.math.pow((new BDV[Double](p.features.toArray).t *wtest._2  - p.label),2)).reduce(_ + _)
      obj = obj/N


      //val gval = points.map(p=>gradfun(p,wtest._2)).reduce(comOp)
      //gnorm = math.sqrt(dotOp(gval,gval))/N
      //println(wtest._1+","+obj+","+gnorm)
      writer.write((wtest._1).toString + ", " + wtest._2.toArray.mkString(",") + "\n" )
      println(wtest._1+","+obj)


    }*/
    //writer.close()
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
    //println("accnum: "+accnum)

    //println("last objective: "+obj)
    //println("average delay:"+avgDelay)
    //println(efficiency)
    println("finished")

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


}

