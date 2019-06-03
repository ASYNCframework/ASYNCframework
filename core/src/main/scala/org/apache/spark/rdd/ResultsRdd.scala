//scalastyle:off
package org.apache.spark.rdd

//import com.oracle.deploy.update.UpdateCheckListener

import java.util.concurrent.LinkedBlockingQueue
import scala.collection.mutable.ListBuffer
import scala.language.implicitConversions
import scala.reflect.{ClassTag, classTag}


class ResultsRdd[T: ClassTag] () extends  Serializable {
  //var ResultsList = new ListBuffer[RDDPartialRes[T]]
  var ResultList = new LinkedBlockingQueue[RDDPartialRes[T]]()
  //var UpdateList = new ListBuffer[T]()
  //var TimeStampList = new ListBuffer[Int]()
  //var RecordsList = new ListBuffer[Int]()
  //var parIndexList = new ListBuffer[Int]()
  //var numFinishedPar = 0
  private var exactRec = false
  private var CurrentTime = 0
  private var LastTime = Int.MinValue


  def setCurrentTime(time: Int): Unit ={
    this.CurrentTime = time
  }

  def getCurrentTime(): Int ={
    CurrentTime
  }

  def setRecordStat(b : Boolean):Unit ={
    exactRec = b
  }

  def getRecordStat():Boolean={
    this.exactRec
  }

  def setLastTime(time: Int): Unit ={
    this.LastTime = time
  }

  def isOld(): Boolean ={
    CurrentTime == LastTime
    /*if (CurrentTime == LastTime){
      true
    }
    else{
      false
    }*/

  }

  def getFromBucket() : RDDPartialRes[T] ={
    this.ResultList.take()
    /*val data = this.ResultsList.headOption
    if(data.isEmpty){
      println("Size is "+ ResultsList.size)

    }
    val d2 = data.get
    this.ResultsList.remove(0)
    d2*/
  }
  /*def getUpdate(): T ={
    //println("Up size and Ts size: "+UpdateList.size+ " "+ TimeStampList.size)

      UpdateList.head
      UpdateList.remove(0)

  }*/

  /*def getTimeStamp():Int = {
    TimeStampList.head
    TimeStampList.remove(0)
  }

  def getRecords():Int = {
    RecordsList.head
    RecordsList.remove(0)
  }

  def getParIndex():Int={
    parIndexList.head
    parIndexList.remove(0)
  }
  def getNumfinishedPar(): Int ={
    numFinishedPar
  }*/
  def getSize():Int={
    //UpdateList.size
    this.ResultList.size()
  }
  /*def printAll(): Unit ={
    println(UpdateList.size+" "+TimeStampList.size+" "+parIndexList.size+" "+RecordsList.size)

  }*/



}


object ResultsRdd {
  def setList[T](setter: RDDPartialRes[T] => Unit, value: RDDPartialRes[T]){setter(value)}
  /*def setUpdateList[T](setter: T => Unit, value: T) {setter(value)}
  def setTimeStampList[Int](setter: Int => Unit, value: Int) {setter(value)}
  def setFields[T](setterUpdate: T => Unit,  setterTime: Int=> Unit, setterRecs: Int => Unit, setterPar:Int=>Unit, res: T , time: Int, recs: Int, parIndex: Int) {
    setterTime(time)
    setterUpdate(res)
    setterRecs(recs)
    setterPar(parIndex)

  }*/

}
