//scalastyle:off
package org.apache.spark.examples
import java.io.{File, PrintWriter}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import breeze.linalg.{DenseVector => BDV, SparseVector => BSV, Vector => BV}
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionModel, LinearRegressionWithSGD}
import org.apache.spark.mllib.feature.Normalizer
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.mllib.BLASUtil._
import BreezeConverters._


object SparkSGDMLLIB {

  def main(args: Array[String]): Unit = {
    println("Spark MLlib SGD application started")


    println("Input arguments:")
    println("Input format: [path name] [file name] [num columns] [num rows] " +
      "[num partitions] [num iterations] [step size] [batch rate] ")
    if(args.length!= 8){
      println("The input arguments are wrong. ")
      return
    }

    val pathname = args(0)
    val fname = args(1)
    val d = args(2).toInt
    var N = args(3).toInt
    val numPart = args(4).toInt
    val numIteration = args(5).toInt
    val stepSize=args(6).toDouble
    val b= args(7).toDouble
    println("path name: " + pathname)
    println("file name: " + fname)
    println("num columns: " + d)
    println("num rows: " + N)
    println("num partitions: " +numPart)
    println("num iterations: " +numIteration)
    println("step size: "+ stepSize)
    println("batch rate: "+b)


    val conf = new SparkConf().setAppName("SGD-MLlib")
      .setMaster("local[8]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    //val data = sc.textFile()
    val comOp: (Vector,Vector)=>Vector = (x,y )=>{
      axpyOp(1.0,x,y)
    }


    val points = MLUtils.loadLibSVMFile(sc, pathname+"/"+fname).repartition(numPart).sample(false,0.1).cache()
    N = points.count().toInt
    val xstart = System.currentTimeMillis()
    // Load training data

    val x = LinearRegressionWithSGD
    val model = x.train(points, numIteration, stepSize,b)
    /*val valuesAndPreds = parsedData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }*/



    val xend = System.currentTimeMillis()

    val optVars = x.h


    println("*********************************")
    //println("time(ms), objective function value, norm of gradient")
    var obj = 0.0
    var gnorm = 0.0
    //val writer = new PrintWriter(new File(pathname+fname+"-SGDMLlib-"+".csv"))

   // for(wtest<-optVars){

      //obj = points.map(p=> scala.math.pow((new BDV[Double](p.features.toArray).t *wtest._2  - p.label),2)).reduce(_ + _)
      //obj=obj/N
      //val gval = points.map(p=>gradfun(p,wtest._2)).reduce(comOp)
      //gnorm = math.sqrt(dotOp(gval,gval))/N
     // val t = wtest._1 - xstart
      //println(t+","+obj+","+gnorm)
      //writer.write((t).toString + ", " + wtest._2.toArray.mkString(",") + "\n" )
      //println(t+","+obj)
    //}
   // writer.close()
      val obj2 = points.map(p=>{
        var x = BDV.zeros[Double](optVars.length)
        var i = 0
        for(wtest<-optVars){
          x(i) = scala.math.pow((new BDV[Double](p.features.toArray).t *wtest._2  - p.label),2)/(N)
          i=i+1
        }
        x

      }).reduce(_+_)

    var i = 0
    for(wtest<-optVars){

      val time = wtest._1-xstart
      println(time+","+obj2(i))
      i=i+1
    }
    println("finished")

    //valuesAndPreds.foreach((result) => println(s"predicted label: ${result._1}, actual label: ${result._2}"))


  }

  def gradfun(p:LabeledPoint, w: Vector): (Vector) ={

    //val z =toBreeze(p.features)
    //val x = new BDV[Double](p.features.toArray)
    //val x= new BSV[Double](p.features.toArray)
    //val y= p.label
    val grad = Vectors.zeros(w.size)
    val diff = dotOp(p.features, w) - p.label
    axpyOp(diff, p.features, grad)
    //val scalar =(z.t * w - y)
    //System.gc()
    //val grad = z*scalar
    grad


  }



}
