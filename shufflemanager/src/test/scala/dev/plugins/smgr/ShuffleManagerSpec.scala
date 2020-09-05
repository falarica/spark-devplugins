package dev.plugins.cmgr

import org.apache.spark.sql.SparkSession
import org.scalatest.{FunSuite, Matchers}

/**
  *
  * @author Hemant Bhanawat
  */
class ShuffleManagerSpec extends FunSuite with Matchers {

  test("test shuffle manager") {
    val spark = SparkSession.builder()
      .master("local[1]")
      .config("spark.shuffle.sort.bypassMergeThreshold", "100")
      .config("spark.shuffle.manager",
        "org.apache.spark.shuffle.sort.SortShuffleManagerWithExtraLoggging")
      .getOrCreate()

    // If we are able to execute this statement that means
    // the cluster manager has been successfully instantiated
    spark.sql("set spark.shuffle.sort.bypassMergeThreshold=500")
    // This will print all the shuffle map files created
    // 10 shuffle files will be created
    spark.range(10).repartition(2).groupBy("id").count().collect()
  }
}
