package dev.plugins.optimizer

import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport}
import org.apache.spark.sql.types.StructType

/**
  * Copied from SimpleDataSourceV2 in Apache Spark
  * A data source that generates sorted data in two partitions
  *
  * @author Hemant Bhanawat
  */
class SortedDataSource extends DataSourceV2 with ReadSupport {
  override def createReader(options: DataSourceOptions): DataSourceReader = new SortedReader
}

class SortedReader extends DataSourceReader {
  override def readSchema(): StructType = new StructType().add("id", "int").add("negativeid", "int")

  override def createDataReaderFactories(): java.util.List[DataReaderFactory[Row]] = {
    java.util.Arrays.asList(new SortedDataReaderFactory(0, 5), new SortedDataReaderFactory(5, 10))
  }
}

class SortedDataReaderFactory(start: Int, end: Int)
  extends DataReaderFactory[Row]
    with DataReader[Row] {
  private var current = start - 1

  override def createDataReader(): DataReader[Row] = new SortedDataReaderFactory(start, end)

  override def next(): Boolean = {
    current += 1
    current < end
  }

  override def get(): Row = Row(current, -current)

  override def close(): Unit = {}
}
