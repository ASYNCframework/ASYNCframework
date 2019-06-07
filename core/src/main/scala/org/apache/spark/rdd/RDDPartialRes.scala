//scalastyle:off
package org.apache.spark.rdd

import scala.reflect.ClassTag

/*
 * This class holds the info for a reduced result on partitions
 * data: the reduced result
 * ts: staleness
 * recs: number of records that have been processed
 * WorkerID: id of the partition
 */
class RDDPartialRes[T: ClassTag] (val data:T,
                            val ts: Int,
                            val recs: Int,
                            val id: Int) extends  Serializable {
  private val TaskResult = data
  private val staleness = ts
  private val batchSize = recs
  private val WorkerID = id

  def getWorkerID(): Int = {
    this.WorkerID
  }

  def getbatchSize(): Int = {
    this.batchSize
  }

  def getStaleness() : Int = {
    this.staleness
  }

  def gettaskResult():T = {
    this.TaskResult
  }
}
