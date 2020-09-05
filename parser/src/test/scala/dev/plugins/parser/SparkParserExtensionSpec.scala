package dev.plugins.parser

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ArrayType, ByteType}
import org.scalatest.{FunSuite, Matchers}

/**
  *
  * @author Hemant Bhanawat
  */
class SparkParserExtensionSpec extends FunSuite with Matchers  {

  test("test blob type") {
    val spark = SparkSession.builder().master("local[1]")
      // add parser session extension to plugin our code
      .withExtensions(new ParserSessionExtension)
      .getOrCreate()

    try {
      cleanParquetFolder(spark)
      // Create a table with blob type
      spark.sql("create table tblWithBlob (mybytearray blob, id int) using parquet options (path '/tmp/p')")

      // insert some dummy data
      spark.sql("Insert Into tblWithBlob values (array(1,2,3), 1)")

      // Check if the type of the blob column is byte array
      spark.sql("select * from tblwithblob").schema.
        exists(s => s.name == "mybytearray" && s.dataType == ArrayType(ByteType)) should be (true)

      // check the data
      (spark.sql("select array_contains(mybytearray, 1) from tblwithblob").collect()(0))
        .getBoolean(0) should be (true)

      spark.sql("drop table tblWithBlob")

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
