//scalastyle:off
package org.apache.spark.broadcast

import java.io.{ObjectInputStream, ObjectOutputStream}

import org.apache.spark.SparkContext

import scala.reflect.ClassTag

/* wrapper lets us update broadcast variables within DStreams' foreachRDD
 without running into serialization issues */
case class ASYNCbroadcast[T: ClassTag](
                                          @transient private val sc: SparkContext,
                                          @transient private val _v: T) {

  @transient private var v = sc.broadcast(_v)


  def value: T = v.value

  def value(index: Int):T = {
    val id = this.getIndex
    this.setID(index)
    val bc_value = v.value
    this.setID(id)
    bc_value
  }

  def getIndex: Long = v.getBroadcastID().broadcastId

  def setID(newID: Long): Unit ={
    v.changeBroadcastID(newID)
  }

  private def writeObject(out: ObjectOutputStream): Unit = {
    out.writeObject(v)
  }

  private def readObject(in: ObjectInputStream): Unit = {
    v = in.readObject().asInstanceOf[Broadcast[T]]
  }




}
