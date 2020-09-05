package dev.plugins.planner

import org.apache.spark.sql.SparkSessionExtensions

/**
  * This class provides a Planner extension to Spark
  *
  * @author Hemant Bhanawat
  */
class PlannerExtension extends (SparkSessionExtensions => Unit) {

  def apply(e: SparkSessionExtensions): Unit = {
    e.injectPlannerStrategy(ModifiedRangeExecStrategy)
  }
}