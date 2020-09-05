package dev.plugins.parser

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.internal.SQLConf

/**
  * This class provides a Parser extension to Spark
  *
  * @author Hemant Bhanawat
  */
class ParserSessionExtension extends (SparkSessionExtensions => Unit) {

  def apply(e: SparkSessionExtensions): Unit = {
    // While injecting parser spark  passes a previous parser builder
    // Here we are ignoring that completely and creating a new one for us.
    e.injectParser((session, parserBuilder) => {
      val conf = new SQLConf
      mergeSparkConf(conf, session.sparkContext.getConf)
      new SparkSqlParserWithDataTypeExtension(conf)
    })
  }
  // Copied from BaseSessionStateBuilder
  protected def mergeSparkConf(sqlConf: SQLConf, sparkConf: SparkConf): Unit = {
    sparkConf.getAll.foreach { case (k, v) =>
      sqlConf.setConfString(k, v)
    }
  }
}