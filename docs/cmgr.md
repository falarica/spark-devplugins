## Goal 

Launch Spark using your cluster manager. 

## Approach

Apache Spark supports cluster managers like YARN and Standalone by default. But it also allows you to plug-in your own cluster manager. For e.g. [Snappydata](https://www.snappydata.io/) has written their own cluster manager so that they can launch the executors in the same process that holds the data in memory. In fact, Apache Spark didn't support cluster manager plugin but when I was part of SnappyData, I contributed [that code to Apache Spark](https://github.com/apache/spark/pull/11723). **Shameless advertising.:-)**

Ideally cluster manager is a separate process, which is responsible for launching executors when driver requests. In this project, for demonstration, instead of talking with an external cluster manager, driver launches the executor processes on the same machine based on configuration.

Following things were done in this project: 

* Added a file org.apache.spark.scheduler.ExternalClusterManager with the name of our cluster manager. This is how you plugin your class. 
* Implemented ExternalClusterManager. The master URL for our project is dev-plugins.
* ModifiedCoarseGrainedSchedulerBackend which is the driver class launches CoarseGrainedExecutorBackend which is the executor class. 

## Try it out

SparkSubmit does not support other cluster managers and hence run the test with this project to try it out. 

## Github 

[Here](https://github.com/falarica/spark-devplugins/tree/master/clustermanager)
