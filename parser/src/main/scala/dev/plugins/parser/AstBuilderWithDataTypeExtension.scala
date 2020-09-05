package dev.plugins.parser

import java.util.Locale

import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.parser.ParserUtils.withOrigin
import org.apache.spark.sql.catalyst.parser.SqlBaseParser.PrimitiveDataTypeContext
import org.apache.spark.sql.execution.SparkSqlAstBuilder
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._

/**
  * Extend SparkSqlAstBuilder and change the primitive datatype visitor
  * to add the support for blob type
  *
  * @author Hemant Bhanawat
  */
class AstBuilderWithDataTypeExtension(conf: SQLConf) extends SparkSqlAstBuilder(conf) {

  override def visitPrimitiveDataType(ctx: PrimitiveDataTypeContext): DataType = {
    withOrigin(ctx) {
      val dataType = ctx.identifier.getText.toLowerCase(Locale.ROOT)
      (dataType, ctx.INTEGER_VALUE().asScala.toList) match {
        // if the string is a blob type return array of byte type.
        case ("blob", Nil) => return ArrayType(ByteType)
        // Rest of the code been copied from the same function in AstBuilder
        case ("boolean", Nil) => BooleanType
        case ("tinyint" | "byte", Nil) => ByteType
        case ("smallint" | "short", Nil) => ShortType
        case ("int" | "integer", Nil) => IntegerType
        case ("bigint" | "long", Nil) => LongType
        case ("float", Nil) => FloatType
        case ("double", Nil) => DoubleType
        case ("date", Nil) => DateType
        case ("timestamp", Nil) => TimestampType
        case ("string", Nil) => StringType
        case ("char", length :: Nil) => CharType(length.getText.toInt)
        case ("varchar", length :: Nil) => VarcharType(length.getText.toInt)
        case ("binary", Nil) => BinaryType
        case ("decimal", Nil) => DecimalType.USER_DEFAULT
        case ("decimal", precision :: Nil) => DecimalType(precision.getText.toInt, 0)
        case ("decimal", precision :: scale :: Nil) =>
          DecimalType(precision.getText.toInt, scale.getText.toInt)
        case (dt, params) =>
          val dtStr = if (params.nonEmpty) s"$dt(${params.mkString(",")})" else dt
          throw new ParseException(s"DataType $dtStr is not supported.", ctx)
      }
    }
  }
}
