package dev.plugins.resolver

import org.apache.spark.sql.SparkSessionExtensions

/**
  * Inject the resolver rule that adds an extra column for month with its name when
  * the month is specified as integer
  *
  * @author Hemant Bhanawat
  */
class ResolverExtension extends (SparkSessionExtensions => Unit) {

  def apply(e: SparkSessionExtensions): Unit = {
    e.injectResolutionRule(AddMonthColumn)
  }
}
