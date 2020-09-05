package dev.plugins.analyzer

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.scalatest.{FunSuite, Matchers}

/**
  *
  * @author Hemant Bhanawat
  */
class EqualityCheckExtensionSpec extends FunSuite with Matchers {

  test("test  type") {
    val spark = SparkSession.builder().master("local[1]")
      .config("spark.sql.extensions",
        "dev.plugins.analyzer.EqualityCheckSessionExtension")
      .getOrCreate()

    import spark.implicits._
    try {
      cleanParquetFolder(spark)

      spark.sql("create table myidtable (value string, id integer) using parquet options (path '/tmp/p')")

      spark.sql("Insert Into myidtable values ('1', 1)")
      spark.sql("Insert Into myidtable values ('2', 2)")
      spark.sql("Insert Into myidtable values ('3', 3)")
      spark.sql("Insert Into myidtable values ('4', 4)")
      spark.sql("Insert Into myidtable values ('5', 5)")

      // an inner join and an equality check are allowed
      spark.sql("select * from myidtable a, myidtable b " +
        "where a.id = b.id and a.value = b.value and a.id = 1").collect.length should be(1)

      spark.sql("select * from myidtable where id = 1").collect().length should be(1)

      spark.sql("select * from myidtable a  inner  join myidtable b on " +
        " a.id = b.id where a.id =1").collect.length should be(1)

      // No filter on ID column
      an[IllegalStateException] should be thrownBy spark.sql("select * from myidtable a, myidtable b " +
        "where a.id = b.id and a.value = b.value").collect
      an[IllegalStateException] should be thrownBy spark.sql("select * from myidtable a, myidtable b " +
        "where a.id = b.id").collect

      // not an inner join
      an[IllegalStateException] should be thrownBy spark.sql("select * from myidtable a  left  join myidtable b on " +
        " a.id = b.id and a.id =1").collect

      // no filter on id column
      an[IllegalStateException] should be thrownBy spark.sql("select * from myidtable a  inner  join myidtable b on " +
        " a.id = b.id where a.value = '1'")

    } finally {
      cleanParquetFolder(spark)
    }

    // For non parquet tables, the behavior remains same as before

    val df = Seq(("1", 1), ("2", 2), ("3", 3), ("4", 4), ("5", 5)).toDF("value", "id")

    df.createTempView("mymemorytable")
    // for non parquet data source, id check is not enforced
    spark.sql("select * from mymemorytable a  inner  join mymemorytable b on " +
      " a.id = b.id where a.value = '1'").collect().length should be (1)

    spark.sql("select * from mymemorytable a, mymemorytable b " +
      "where a.id = b.id and a.value = b.value").collect.length should be (5)

    spark.sql("select * from mymemorytable a, mymemorytable b " +
      "where a.id = b.id").collect.length should be (5)

    // for non parquet data source, inner join check is enforced.
    an[IllegalStateException] should be thrownBy spark.sql("select * from mymemorytable a  left  join mymemorytable b on " +
      " a.id = b.id and a.id =1 ").collect

    spark.sql("select * from mymemorytable a  inner  join mymemorytable b on " +
      " a.id = b.id where a.value = '1'").collect.length should be (1)

  }

  private def cleanParquetFolder(spark: SparkSession) = {
    val root = new Path("/tmp/p")
    val fs = root.getFileSystem(spark.sparkContext.hadoopConfiguration)
    fs.delete(root, true)
  }
}
