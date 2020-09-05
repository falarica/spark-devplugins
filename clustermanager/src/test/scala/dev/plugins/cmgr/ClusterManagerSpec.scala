package dev.plugins.cmgr

import org.apache.spark.sql.SparkSession
import org.scalatest.{FunSuite, Matchers}

/**
  *
  * @author Hemant Bhanawat
  */
class ClusterManagerSpec extends FunSuite with Matchers {

  test("test cluster manager") {
    val spark = SparkSession.builder()
      .master("devplugins")
      .config("spark.dev.plugins.cmgr.numExecutors", "2")
      .config("spark.dev.plugins.cmgr.numCores", "3")
      .getOrCreate()

    // If we are able to execute this statement that means
    // the cluster manager has been successfully instantiated
    spark.range(10).collect.length should be (10)
  }
}
