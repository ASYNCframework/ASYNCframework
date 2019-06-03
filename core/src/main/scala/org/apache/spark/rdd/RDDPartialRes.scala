//scalastyle:off
package org.apache.spark.rdd

import scala.reflect.ClassTag

/*
 * This class holds the info for a reduced result on partitions
 * data: the reduced result
 * ts: time stamp
 * recs: number of records that have been processed
 * index: index of the partition
 */
class RDDPartialRes[T: ClassTag] (val data:T,
                            val ts: Int,
                            val recs: Int,
                            val index: Int) extends  Serializable {
  private val partialResult = data
  private val timeStamp = ts
  private val recordNum = recs
  private val partitionIndex = index

  def getPartitionIndex(): Int = {
    this.partitionIndex
  }

  def getRecordsNum(): Int = {
    this.recordNum
  }

  def getTimeStamp() : Int = {
    this.timeStamp
  }

  def getData():T = {
    this.partialResult
  }
}
