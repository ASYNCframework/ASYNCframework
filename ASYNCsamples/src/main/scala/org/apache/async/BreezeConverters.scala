//scalastyle:off
package org.apache.async

import breeze.linalg.{DenseVector => BDV, SparseVector => BSV, Vector => BV}
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector}

import scala.language.implicitConversions

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