package dev.plugins.optimizer

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.SortExec
import org.scalatest.{FunSuite, Matchers}

/**
  *
  * @author Hemant Bhanawat
  */
class OptimizeSortSpec extends FunSuite with Matchers {

  test("test for sort operator") {
    val spark = SparkSession.builder().master("local[1]")
      // add parser session extension to plugin our code
      .config("spark.sql.extensions",
      "dev.plugins.optimizer.OptimizerExtension")
      .getOrCreate()
    import spark.implicits._

    // Create a sorted data source's table
    val df = spark.read.format(classOf[SortedDataSource].getName).load()
    df.createTempView("sortedTable")

    // An order by query on the sorted data source, should have its sort order removed
    // from the executed plan
    val df1 = spark.sql("select * from sortedTable where id % 2 = 0 order by id")
    df1.queryExecution.executedPlan.find(_.isInstanceOf[SortExec]).isDefined should be(false)

    // A join query that does a sort merge join, should still be sorting its data, because
    // it does sorting and shuffling.
    val df2 = spark.sql("select * from sortedTable a, sortedTable b where a.id = b.id")
    df2.queryExecution.executedPlan.find(_.isInstanceOf[SortExec]).isDefined should be(true)

    // An order by query but descending should also sort the data
    val df3 = spark.sql("select * from sortedTable where id % 2 = 0 order by id desc ")
    df3.queryExecution.executedPlan.find(_.isInstanceOf[SortExec]).isDefined should be(true)

    Seq(("1", 1), ("2", 2), ("3", 3), ("4", 4), ("5", 5)).toDF("value", "id").createTempView("mymemorytable")

    // An order by query on any datasource other than sorted data source should do sorting
    val df4 = spark.sql("select * from mymemorytable where id % 2 = 0 order by id")
    df4.queryExecution.executedPlan.find(_.isInstanceOf[SortExec]).isDefined should be(true)

  }

}
