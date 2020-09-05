package dev.plugins.parser.app

import dev.plugins.parser.ParserSessionExtension
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

/**
  *
  * @author Hemant Bhanawat
  */
object Run {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder
      .withExtensions(new ParserSessionExtension)
      .master("local")
      .appName("Parser For Blob")
      .getOrCreate()
    try {
      spark.sql("create table tblWithBlob (mybytearray blob) using parquet options (path '/tmp/p')")

      spark.sql("Insert Into tblWithBlob values (array(1,2,3))")

      println(s"Schema of table is ${spark.sql("select * from tblwithblob").schema}")

      spark.sql("select * from tblwithblob").show

      spark.sql("drop table tblWithBlob")
    } finally {
      val root = new Path("/tmp/p")
      val fs = root.getFileSystem(spark.sparkContext.hadoopConfiguration)
      fs.delete(root, true)
    }

  }

}
