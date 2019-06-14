//scalastyle:off
package org.apache.spark.rdd

import scala.reflect.ClassTag


/*
 * This class holds the state of a worker
 * worker state consists of :
 * 1) staleness,
 * 2) average task completion time(ms),
 * 3) availability
 */
class workerState [T](AC :ASYNCcontext[T],
                      stale: Int,
                      time:Long,
                      avail :Boolean)  extends Serializable {


  //private var AC:ASYNCcontext[T]
  private var staleness = stale
  private var averageTaskTime = time
  private var availability = avail
  private var numTasks = 0 // number of executed tasks until now
  private var Max_Staleness = 0 // max staleness among all workers
  private var Available_Workers = 0 // number of available workers


  def this(AC:ASYNCcontext[T]){
    this(AC,0,0L,false)
  }




  def updateNumTasks(n : Int):Unit = {
    this.numTasks = this.numTasks + n
  }
  def setStaleness(s: Int): Unit={
    this.staleness = s
  }

  def setAverageTaskTime(t: Long): Unit={
    this.averageTaskTime = t
  }

  def setAvailability(a : Boolean): Unit={
    this.availability = a
  }

  def getNumTasks(): Int ={
    this.numTasks
  }

  def getAvailability():Boolean={
    this.availability
  }

  def getStaleness():Int={
    this.staleness
  }

  def getAvailableWorkers(): Int={
    var n = 0
    for(s<-AC.STAT){
      if(s._2.getAvailability()==true){
        n=n+1
      }
    }
    Available_Workers = n
    n
  }

  def getMaxStaleness(): Int={
    var n = -1
    for(s<-AC.STAT){
      if( s._2.getStaleness() > n)
        n = s._2.getStaleness()
    }
    Max_Staleness = n
    n
  }




}
