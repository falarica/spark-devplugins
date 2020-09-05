package dev.plugins.parser

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.dev.plugins.ModifiedRangeExec
import org.scalatest.{FunSuite, Matchers}

/**
  *
  * @author Hemant Bhanawat
  */
class SparkPlannerExtensionSpec extends FunSuite with Matchers {

  test("test modified range exec operator") {
    val spark = SparkSession.builder().master("local[1]")
      // add parser session extension to plugin our code
      .config("spark.sql.extensions",
      "dev.plugins.planner.PlannerExtension")
      .getOrCreate()


    val df = spark.range(start = 0, end = 10, step = 3, numPartitions = 2)
    df.collect should be(Array(0, 3, 6, 9))
    df.queryExecution.executedPlan.find(_.isInstanceOf[ModifiedRangeExec]).isDefined should be(true)

    val df1 = spark.range(start = 1, end = 10, step = 3, numPartitions = 2)
    df1.collect should be(Array(3, 6, 9))

    val df2 = spark.range(start = 2, end = 10, step = 3, numPartitions = 2)
    df2.collect should be(Array(3, 6, 9))

    val df3 = spark.range(start = 3, end = 10, step = 3, numPartitions = 2)
    df3.collect should be(Array(3, 6, 9))

    val df4 = spark.range(start = 210, end = 500, step = 147, numPartitions = 2)
    df4.collect() should be(Array(294, 441))
  }
}
