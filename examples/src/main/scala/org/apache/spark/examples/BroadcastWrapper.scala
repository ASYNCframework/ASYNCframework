//scalastyle:off
package  org.apache.spark.examples


import java.io.{ObjectInputStream, ObjectOutputStream}

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.StreamingContext

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag
import scala.collection.mutable.HashMap


/* wrapper lets us update brodcast variables within DStreams' foreachRDD
 without running into serialization issues */
case class BroadcastWrapper[T: ClassTag](
                                          @transient private val sc: SparkContext,
                                          @transient private val _v: T) {

  @transient private var v = sc.broadcast(_v)


  def value: T = v.value

  def index: Long = v.getBroadcastID().broadcastId

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