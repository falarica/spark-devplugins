package dev.plugins.resolver

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.UnresolvedStar
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.catalyst.expressions._
/**
  * Add an extra column for the data source that has two fields salary and bonus
  * wiith name syscol_yearly_takeaway
  *
  * @author Hemant Bhanawat
  */
case class AddMonthColumn(spark: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    // return if the desired field has already been added
    plan foreach {
      case a@Project(projectList, _)
        if projectList.exists(p => p.resolved && p.name == "syscol_yearly_takeaway") =>
        return plan
      case _ =>
    }

    plan transformUp {
      case lr@LogicalRelation(fsr: HadoopFsRelation, output, _, _)
        if output.exists(_.name.toLowerCase() == "salary") &
          output.exists(_.name.toLowerCase() == "bonus") =>

        val sal = output.find(_.name.toLowerCase() == "salary").get
        val bonus = output.find(_.name.toLowerCase() == "bonus").get
        // insert a Project node in the plan for our datasource with an extra field
        // which is sum of two other columns
        Project(Seq(UnresolvedStar(None), Alias(Add(sal, bonus), "syscol_yearly_takeaway")()), lr)
    }
  }

}

