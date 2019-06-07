//scalastyle:off
package org.apache.spark.rdd


/*
 * This class holds the state of a worker
 * worker state consists of :
 * 1) staleness,
 * 2) average task completion time(ms),
 * 3) availability
 */
class workerState () {

  private var staleness = 0
  private var averageTaskTime = 0L
  private var availability = false
  private var numTasks = 0 // number of executed tasks until now
  private var Max_Staleness = 0 // max staleness among all workers
  private var Available_Workers = 0 // number of available workers

  def this(stale: Int, time:Long, avail :Boolean){
    this()
    this.staleness = stale
    this.averageTaskTime = time
    this.availability = avail
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

  def setAvailableWorkers(n : Int): Unit={
    this.Available_Workers = n
  }

  def setMaxStaleness(s: Int): Unit={
    this.Max_Staleness = s
  }

  def getNumTasks(): Int ={
    this.numTasks
  }




}
