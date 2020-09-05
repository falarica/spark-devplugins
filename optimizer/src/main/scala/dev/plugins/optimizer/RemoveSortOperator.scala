package dev.plugins.optimizer

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Ascending, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Sort}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

/**
  * Function that transforms a logical plan such that any global ascending sort operator on
  * the our SortedDataSource is removed. Local sort operators are however allowed
  * because they are needed for sort merge join
  * @author Hemant Bhanawat
  */
case class RemoveSortOperator(spark: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan transformDown {
      // Check for ascending global Sort.
      case sort@Sort(order, global, child) if global & order.length == 1 &
        order(0).direction == Ascending =>
        var hasSortedDataSource = false

        child transformDown {
          // Check that the sort operator has our datasource as a child node
          case a@DataSourceV2Relation(_, reader: SortedReader) =>
            if (order(0).child.isInstanceOf[AttributeReference] &&
              order(0).child.asInstanceOf[AttributeReference].name == "id") {
              hasSortedDataSource = true
            }
            a
        }
        // If it has sorted datasource return the child of it so that the sort operator
        // is remove from the tree.
        if (hasSortedDataSource) child else sort
    }
  }

}

