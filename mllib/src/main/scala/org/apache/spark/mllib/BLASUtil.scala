//scalastyle:off
package org.apache.spark.mllib
import org.apache.spark.mllib.linalg.{ Vector}
import org.apache.spark.mllib.linalg.BLAS.{axpy, dot, scal}

object BLASUtil {
  def axpyOp(a:Double, x:Vector, y:Vector):Vector={
    axpy(a, x, y)
    y
  }
  def dotOp(x:Vector, y:Vector):Double={
    dot(x, y)
  }
  def scalOp(a:Double, x:Vector):Vector={
    scal(a,x)
    x
  }

}
