package dev.plugins.analyzer

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, EqualTo, Literal, Or}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}

/**
  *
  * @author Hemant Bhanawat
  */
case class EqualityCheckExtension(spark: SparkSession) extends (LogicalPlan => Unit) {
  override def apply(plan: LogicalPlan): Unit = {
    // Return if it is not a select query
    if (!isASelectQuery(plan)) return

    // Iterate through all the nodes of the plan which is an AST
    plan.foreach {
      case operator@Filter(condition, childPlan) =>
        // For a filter node with a parquet data source
        if (verifyItsAParquetDataSrc(childPlan)) {
          var idFilterPresent = false
          // get the condition of filter node and check if it has
          // an equality check on id column
          condition transformDown {
            case c@EqualTo(left, right) =>
              left match {
                case a@AttributeReference(name, _, _, _)
                  if name == "id" & right.isInstanceOf[Literal] =>
                  idFilterPresent = true
                case _ =>
              }
              right match {
                case a@AttributeReference(name, _, _, _)
                  if name == "id" & left.isInstanceOf[Literal] =>
                  idFilterPresent = true
                case _ =>
              }
              c
            case e =>
              e
          }
          // if filter is not present throw an exception
          if (!idFilterPresent)
            throw new IllegalStateException("All queries should have a equality query.")
        }
      case operator@Join(left, right, joinType, condition) =>
        // For the join nodes if the join type is not inner, throw an exception
        if (joinType != Inner) {
          throw new IllegalStateException("Join is not of inner type")
        }
      case _ =>
    }

  }

  private def isASelectQuery(plan: LogicalPlan): Boolean = {
    // If the topmost node is Project, we assume it is a select query
    return plan match {
      case Project(_, _) => true
      case _ => true
    }
  }

  private def verifyItsAParquetDataSrc(plan: LogicalPlan): Boolean = {
    plan.foreach {
      case a@LogicalRelation(relation: HadoopFsRelation, output, _, _) =>
        return relation.dataSchema.exists(_.name == "id") &&
          relation.fileFormat.isInstanceOf[ParquetFileFormat]
      case _ =>
    }
    false
  }
}
