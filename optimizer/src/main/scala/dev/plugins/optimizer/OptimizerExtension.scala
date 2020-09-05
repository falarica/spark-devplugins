package dev.plugins.optimizer

import org.apache.spark.sql.SparkSessionExtensions

/**
  * Inject the optimizer rule that removes the sort operator for
  * an already sorted data source
  *
  * @author Hemant Bhanawat
  */
class OptimizerExtension extends (SparkSessionExtensions => Unit) {

  def apply(e: SparkSessionExtensions): Unit = {
    e.injectOptimizerRule(RemoveSortOperator)
  }
}
