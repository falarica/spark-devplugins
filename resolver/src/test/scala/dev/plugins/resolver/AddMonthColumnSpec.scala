package dev.plugins.optimizer

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.scalatest.{FunSuite, Matchers}

/**
  *
  * @author Hemant Bhanawat
  */
class AddMonthColumnSpec extends FunSuite with Matchers {

  test("test month column") {
    val spark = SparkSession.builder().master("local[1]")
      // add parser session extension to plugin our code
      .config("spark.sql.extensions",
      "dev.plugins.resolver.ResolverExtension")
      .getOrCreate()

    try {
      cleanParquetFolder(spark)
      spark.sql("create table salary (id string, salary integer, bonus integer) using parquet options (path '/tmp/p')")

      spark.sql("Insert Into salary values ('1', 100000, 10000)")
      spark.sql("Insert Into salary values ('2', 200000, 15000)")
      spark.sql("Insert Into salary values ('3', 150000, 30000)")
      spark.sql("Insert Into salary values ('4', 90000, 25000)")
      spark.sql("Insert Into salary values ('5', 80000, 50000)")

      spark.sql("select * from salary where syscol_yearly_takeaway >= 130000").collect.length should be(3)
      spark.sql("select * from salary").schema.exists(_.name == "syscol_yearly_takeaway") should be (true)
      spark.sql("select a.* from salary a, salary b where a.id = b.id").
        schema.exists(_.name == "syscol_yearly_takeaway") should be (true)
      spark.sql("select a.* from salary a, salary b where a.id = b.id").collect().length should be (5)

    } finally {
      cleanParquetFolder(spark)
    }
  }
  private def cleanParquetFolder(spark: SparkSession) = {
    val root = new Path("/tmp/p")
    val fs = root.getFileSystem(spark.sparkContext.hadoopConfiguration)
    fs.delete(root, true)
  }


}
