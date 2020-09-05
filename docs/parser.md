## Goal 

Support "blob" keyword in Apache Spark SQL. If someone specifies a "blob" keyword, it should be converted that to Array[Byte] internally. 

## Approach

Apache Spark SQL parser is based on ANTLR. Here is a wonderful tutorial on [how ANTLR works](https://tomassetti.me/antlr-mega-tutorial/). Spark has written visitor functions to return the tree nodes while processing. 

Spark allows injection of a new parser instead of the default one. For supporting blob types, following things are done:

* Added ParserSessionExtension which is used to inject our parser

* Injected SparkSqlParserWithDataTypeExtension which replaces the default SparkSqlParser. This class replaces the default abstract syntax tree builder with AstBuilderWithDataTypeExtension

* AstBuilderWithDataTypeExtension overrides visitPrimitiveDataType and adds support for blob type

## Try it out

```bash 
$ bin/spark-shell --jars <PATH>/parser-0.1.0-SNAPSHOT.jar --conf spark.sql.extensions=dev.plugins.parser.ParserSessionExtension 

scala> spark.sql("create table tblWithBlob (mybytearray blob, id int) using parquet options (path '/tmp/p')")

scala> spark.sql("Insert Into tblWithBlob values (array(1,2,3), 1)")

scala> spark.sql("select * from  tblwithblob").schema
res3: org.apache.spark.sql.types.StructType = StructType(StructField(mybytearray,ArrayType(ByteType,true),true), StructField(id,IntegerType,true))

scala> spark.sql("select * from  tblwithblob").show 
+-----------+---+
|mybytearray| id|
+-----------+---+
|  [1, 2, 3]|  1|
+-----------+---+

```

## Github 

[Here](https://github.com/falarica/spark-devplugins/tree/master/parser)
