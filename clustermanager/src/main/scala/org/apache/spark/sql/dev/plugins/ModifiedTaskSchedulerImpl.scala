package org.apache.spark.sql.dev.plugins

import org.apache.spark.SparkContext
import org.apache.spark.scheduler.TaskSchedulerImpl

/**
  * This is an dummy implementation just for demonstration
  * You can override the functions of TaskSchedulerImpl to alter the behavior
  * of the scheduler
  *
  * @author Hemant Bhanawat
  */
class ModifiedTaskSchedulerImpl(sc: SparkContext)
  extends TaskSchedulerImpl(sc) {

}
