package dev.plugins.analyzer

import org.apache.spark.sql.SparkSessionExtensions

/**
  * Inject the equality check and join check extension
  *
  * @author Hemant Bhanawat
  */
class EqualityCheckSessionExtension extends (SparkSessionExtensions => Unit) {

  def apply(e: SparkSessionExtensions): Unit = {
    e.injectCheckRule(EqualityCheckExtension)
  }
}
