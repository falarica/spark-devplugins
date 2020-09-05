package dev.plugins.planner

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.dev.plugins._
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy}
/**
  * The Strategy class that changes the logical plan to physical plan
  *
  * Since the injected strategies are applied before system strategies I am able to replace
  * Range logical plan to ModifiedRangeExec 
  *
  * @author Hemant Bhanawat
  */
case class ModifiedRangeExecStrategy(spark: SparkSession) extends SparkStrategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case r: logical.Range =>
      ModifiedRangeExec(r) :: Nil
    case _ => Nil

  }
}

