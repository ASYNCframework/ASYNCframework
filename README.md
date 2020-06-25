# ASYNC frameowrk

ASYNC is built on top of Spark 2.3.2, a fast and general cluster computing system for Big Data. It provides an API in Scala(2.11) for implementing asynchronous optimization methods. The compilation is the same as Apache Spark. You can find the latest Spark documentation, including a programming guide, on the [project web page](http://spark.apache.org/documentation.html).

# Presentation
Please check the [Presentation](https://github.com/ASYNCframework/ASYNCframework/tree/master/Presentation) directory for slides and video of the presentation.


# Compiling ASYNC

ASYNC has the same compilation process as Spark. For more info about compiling Spark please refer to https://spark.apache.org/docs/2.3.2/building-spark.html. Here we briefly mention building ASYNC using **SBT**.

## Building with SBT
Maven is the official build tool recommended for packaging Spark, and is the build of reference. But SBT is supported for day-to-day development since it can provide much faster iterative compilation. More advanced developers may wish to use SBT.

The SBT build is derived from the Maven POM files, and so the same Maven profiles and variables can be set to control the SBT build. For example:

```sh
./build/sbt package
```


# Tests
ASYNC comes with two asynchronous optimization algorithm, Asynchronous Stochastic Gradient Descent [ASGD](https://papers.nips.cc/paper/4687-large-scale-distributed-deep-networks.pdf) and [ASAGA](http://proceedings.mlr.press/v54/leblond17a/leblond17a.pdf) which are located in the `ASYNCsamples` directory:

- SparkASAGAThread: The synchronous version of SAGA (ASAGA) with history.

- SparkASAGASync: The synchronous version of SAGA with history.

- SparkASGDThread: The asynchronous version of Stochastic Gradient Descent (ASGD).

- SparkASGDSync: The synchronous version of Stochastic Gradient Descent (SGD).

- SparkSGDMLLIB: The MLlib implementation of SGD for comparison.

The template for running each method, or any new developed one is identical running a Spark application:

```sh
./bin/spark-submit [Spark Args] --class <class> [params]
```

# Complete guide for all experiments in the paper

We ran two optimization methods, synchronous stochastic gradient descent (SGD) and SAGA and their asynchronous variants (ASGD and ASAGA) on the XSEDE Comet CPUs with our ASYNC framework and compared it to the synchronous implementation of Mllib. All experiments use Scala 2.11 and Spark 2.3.2. We use three datasets for evaluation of our framework all of which are publically available.

In order to run the experiments, the code should be compiled to generate jar files that then execute with spark-submit. Evaluation scripts are provided to simplify the execution of the experiments. We use 9 compute nodes from Comet, one for master node and eight as workers. Each worker has one executor with 2 cores. The driver memory is set to 15 Gb and the executor memory is set to 13Gb in all experiments.

The implemented algorithms have the following input parameters: `[path] [file name] [#columns] [#rows] [#partitions] [#iterations] [step size] [sampling rate] [beta] [delay intensity] [seed]`

In order to generate the figures in the paper, the parameters should be set as follows; all parameters are tuned. If a parameter is not mentioned for an algorithm, it means it is not part of that algorithm. Also, some parameters are fixed throughout all experiments: #partitions = 32, beta = 0.93, seed = 42.

**Figure 2**. for mnist8m > dataset:#iteration = 10000, step size = 8e-7, sampling rate = 0.1 for epsilon> dataset:#iteration = 3000, step size = 10, sampling rate = 0.1 for rcv1_full.bianry > dataset:#iteration = 3000, step size = 10, sampling rate = 0.05

**Figure 3**. In this figure delay intensity is chosento be [0.3, 0.6, 1.0] to generate all results for each dataset. Other parameters are: For the ASYNC results: for mnist8m > dataset:#iteration = 300000, step size = 0.25e-7, sampling rate = 0.1 for epsilon> dataset:#iteration = 320000, step size = 0.3125, sampling rate = 0.1 for rcv1_full.bianry > dataset:#iteration = 100000, step size = 0.3125, sampling rate = 0.05 For the Sync results: for mnist8m > dataset:#iteration = 10000, step size = 8e-7, sampling rate = 0.1 for epsilon> dataset:#iteration = 10000, step size = 10, sampling rate = 0.1 for rcv1_full.bianry > dataset:#iteration = 3000, step size = 10, sampling rate = 0.05

**Figure 4** is generated from the outputs of the experiments done in Figure 3.

**Figure 5**. In this figure delay intensity is chosen from [0.3, 0.6, 1.0] to generate the results for each dataset. Other parameters are: For the ASYNC results: for mnist8m > dataset:#iteration = 45000, step size = 0.1875e-7, sampling rate = 0.01 for epsilon> dataset:#iteration = 130000, step size = 0.03125, sampling rate = 0.1 for rcv1_full.bianry > dataset:#iteration = 100000, step size = 0.3125, sampling rate = 0.02 For the Sync results: for mnist8m > dataset:#iteration = 1500, step size = 6e-7, sampling rate = 0.01 for epsilon> dataset:#iteration = 4000, step size = 1, sampling rate = 0.1 for rcv1_full.bianry > dataset:#iteration = 3000, step size = 10, sampling rate = 0.02

**Figure 6** is generated from the outputs of the experiments in Figure 5.

**For Figures 7 and 8** we use 32 workers, each has one executor with 2 cores and 5Gb of memory. The parameter beta is set to 0.7 and delay intensity is set to -1 and batch rate is set to 0.1 for both figures. Other parameters are: Figure 7: mnist8m-ASYNC: #iteration = 420000 [step size]:3.3333e-3 mnist8m-Sync: #iteration = 14000 [step size]:0.1 epsilon-ASYNC:#iteration = 40000 [step size]:3.33e-1 epsilon-Sync:#iteration = 13000 [step size]:10 Figure 8: mnist8m-ASYNC: #iteration = 12000[step size]:1.3333e-3 mnist8m-Sync: #iteration = 4000[step size]:0.04 epsilon-ASYNC:#iteration = 30000 [step size]:3.33e-2 epsilon-Sync:#iteration = 10000 [step size]:1

**Table 3** is generated based on the outputs of Figure 7 and 8.
