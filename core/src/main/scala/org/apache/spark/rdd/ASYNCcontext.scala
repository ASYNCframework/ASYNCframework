//scalastyle:off
package org.apache.spark.rdd

//import com.oracle.deploy.update.UpdateCheckListener

import java.util.concurrent.LinkedBlockingQueue

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.language.implicitConversions
import scala.reflect.{ClassTag, classTag}


class ASYNCcontext[T: ClassTag]() extends  Serializable {
  var ResultList = new LinkedBlockingQueue[RDDPartialRes[T]]()
  val STAT = new mutable.HashMap[Int,workerState]()
  //Initialize(STAT)

  private var exactRec = false
  private var CurrentTime = 0
  private var LastTime = Int.MinValue


  def setCurrentTime(time: Int): Unit ={
    this.CurrentTime = time
  }

  def add2currentTime(t: Int): Unit = {
    this.CurrentTime = this.CurrentTime + t
  }

  def getCurrentTime(): Int ={
    this.CurrentTime
  }

  def setRecordStat(b : Boolean):Unit ={
    this.exactRec = b
  }

  def getRecordStat():Boolean={
    this.exactRec
  }

  def setLastTime(time: Int): Unit ={
    this.LastTime = time
  }

  def isOld(): Boolean ={
    this.CurrentTime == this.LastTime
  }

  def getFromBucket() : RDDPartialRes[T] ={
    this.ResultList.take()
  }

  def getSize():Int={
    this.ResultList.size()
  }


}


object ASYNCcontext {
  def updateTaskResultList[T](setter: RDDPartialRes[T] => Unit, value: RDDPartialRes[T]){setter(value)}
  def updateSTAT[T](setter: (Int, workerState) => Unit, index: Int, value: workerState){setter(index,value)}

}
