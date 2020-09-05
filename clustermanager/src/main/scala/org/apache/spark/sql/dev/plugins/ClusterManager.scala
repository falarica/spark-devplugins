package org.apache.spark.sql.dev.plugins

import org.apache.spark.SparkContext
import org.apache.spark.scheduler.{ExternalClusterManager, SchedulerBackend, TaskScheduler}

/**
  *
  * @author Hemant Bhanawat
  */
class ClusterManager extends ExternalClusterManager {

  def canCreate(masterURL: String): scala.Boolean = {
    masterURL.equals("devplugins")
  }

  def createTaskScheduler(sc: SparkContext,
    masterURL: String): org.apache.spark.scheduler.TaskScheduler = {
    new ModifiedTaskSchedulerImpl(sc)
  }

  def createSchedulerBackend(sc: SparkContext, masterURL: String,
    scheduler: TaskScheduler): SchedulerBackend = {
    val numExecutors = sc.conf.get("spark.dev.plugins.cmgr.numExecutors", "1").toInt
    val numCores = sc.conf.get("spark.dev.plugins.cmgr.numCores", "4").toInt
    new ModifiedCoarseGrainedSchedulerBackend(sc,
      scheduler.asInstanceOf[ModifiedTaskSchedulerImpl],
      numCores, numExecutors)
  }

  def initialize(scheduler: TaskScheduler, backend: SchedulerBackend): Unit = {
    val schedulerImpl = scheduler.asInstanceOf[ModifiedTaskSchedulerImpl]
    schedulerImpl.initialize(backend)
  }

}
