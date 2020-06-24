# ASYNC frameowrk

ASYNC is built on top of Spark 2.3.2, a fast and general cluster computing system for Big Data. It provides an API in Scala(2.11) for implementing asynchronous optimization methods. The compilation is the same as Apache Spark. You can find the latest Spark documentation, including a programming guide, on the [project web page](http://spark.apache.org/documentation.html).


## Compiling ASYNC

ASYNC has the same compilation process as Spark. For more info about compiling Spark please refer to https://spark.apache.org/docs/2.3.2/building-spark.html


## Tests
ASYNC comes with two asynchronous optimization algorithm, Asynchronous Stochastic Gradient Descent [ASGD](https://papers.nips.cc/paper/4687-large-scale-distributed-deep-networks.pdf) and [ASAGA](http://proceedings.mlr.press/v54/leblond17a/leblond17a.pdf) which are located in the `ASYNCsamples` directory:

-SparkASAGAThread: The synchronous version of SAGA (ASAGA) with history.

-SparkASAGASync: The synchronous version of SAGA with history.

-SparkASGDThread: The asynchronous version of Stochastic Gradient Descent (ASGD).

-SparkASGDSync: The synchronous version of Stochastic Gradient Descent (SGD).

-SparkSGDMLLIB: The MLlib implementation of SGD for comparison.

To run each method, simply run:

```sh
./bin/spark-submit [Spark Args] --class <class> [params]
```

For example, 


> **_NOTE:_** To see some examples please refer to scripts files in ASYNCsamples directory
