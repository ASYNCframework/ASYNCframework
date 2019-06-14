# ASYNC frameowrk

ASYNC is built on top of Spark 2.3.2, a fast and general cluster computing system for Big Data. It provides an API in Scala(2.11) for implementing asynchronous optimization methods. The compilation is the same as Apache Spark. You can find the latest Spark documentation, including a programming guide, on the [project web page](http://spark.apache.org/documentation.html).


## Compiling ASYNC

ASYNC has the same compilation process as Spark. For more info about compiling Spark please refer to https://spark.apache.org/docs/2.3.2/building-spark.html


## Example Programs
ASYNC comes with two asynchronous optimization algorithm, Asynchronous Stochastic Gradient Descent(ASGD)[] and ASAGA[]. These programs are located in the ASYNCsamples directory:

SparkASAGAThread: ASAGA

SparkASAGASync: SAGA

SparkASGDThread: ASGD

SparkASGDSync: SGD

SparkSGDMLLIB: the MLlib implementation of SGD for comparison

To run one of them, use ./bin/spark-submit [Spark Args] --class <class> [params]. To see some examples please refer scripts file.
