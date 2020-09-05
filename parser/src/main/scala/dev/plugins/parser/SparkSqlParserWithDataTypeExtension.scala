package dev.plugins.parser

import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.internal.SQLConf

/**
  * The new Parser class for adding blob type. Extend SparkSqlParser and change
  * the abstract syntax tree builder to add the support.
  *
  * @author Hemant Bhanawat
  */
class SparkSqlParserWithDataTypeExtension(conf: SQLConf) extends SparkSqlParser(conf) {

  override val astBuilder = new AstBuilderWithDataTypeExtension(conf)

}
