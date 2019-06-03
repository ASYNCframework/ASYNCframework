//scalastyle:off
package org.apache.spark.examples

import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.mllib.linalg.Vector
import scala.language.implicitConversions

import breeze.linalg.{DenseVector => BDV, SparseVector => BSV, Vector => BV}

object BreezeConverters
{

  implicit def toBreeze( v: Vector ): BV[Double] =
    v match {
      case dv: DenseVector => new BDV[Double](dv.values)
      case sv: SparseVector => new BSV[Double](sv.indices, sv.values, sv.size)
    }

  implicit def fromBreeze( dv: BDV[Double] ): DenseVector =
    new DenseVector(dv.toArray)

  implicit def fromBreeze( sv: BSV[Double] ): SparseVector =
    new SparseVector(sv.length, sv.index, sv.data)

  implicit def fromBreeze( bv: BV[Double] ): Vector =
    bv match {
      case dv: BDV[Double] => fromBreeze(dv)
      case sv: BSV[Double] => fromBreeze(sv)
    }
}