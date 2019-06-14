//scalastyle:off
package org.apache.spark.rdd

import java.util.concurrent.LinkedBlockingQueue

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast

import scala.collection.mutable
import scala.language.implicitConversions
import scala.reflect.ClassTag
import org.apache.spark.broadcast._

class ASYNCcontext[T: ClassTag]() extends  Serializable {
  val ResultList = new LinkedBlockingQueue[RDDPartialRes[T]]()
  val STAT = new mutable.HashMap[Int,workerState[T]]()
  //Initialize(STAT)
  /*def ASYNCbroadcast[T: ClassTag](value: T): BroadcastWrapper[T] = {
    new BroadcastWrapper(sc,value)
  }*/


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

  def ASYNCcollect() : T ={
    this.ResultList.take().gettaskResult()
  }

  def ASYNCcollectAll(): RDDPartialRes[T]={
    this.ResultList.take()
  }

  def getSize():Int={
    this.ResultList.size()
  }


  def hasNext():Boolean={
    !this.ResultList.isEmpty
  }


}


object ASYNCcontext {
  def updateTaskResultList[T](setter: RDDPartialRes[T] => Unit, value: RDDPartialRes[T]){setter(value)}
  def updateSTAT[T](setter: (Int, workerState[T]) => Unit, index: Int, value: workerState[T]){setter(index,value)}

}
