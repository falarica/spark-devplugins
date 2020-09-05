## Goal 

Remove un-necessary sorting on an already sorted data source

## Approach

Apache Spark's parser generates a abstract syntax tree. The tree nodes are the operators of a query. The AST is passed through various phases of query compilation i.e. analysis, optimizer, planner and execution. In this exercise, we inject an optimizer rule. 

Let's say we have a datasource (created in this project) that is already sorted on a particular column. An order by query on this column of the datasource should not result in a sort operation. We can achieve this by adding an optimizer rule that does the following: 

* Recognizes the global ascending sort operator

* checks if it has a child that is our sorted datasource, remove the sort operator

## Try it out

```bash 
$ bin/spark-shell --jars <PATH>/optimizer-0.1.0-SNAPSHOT.jar --conf spark.sql.extensions=dev.plugins.optimizer.OptimizerExtension 

scala> spark.read.format(classOf[dev.plugins.optimizer.SortedDataSource].getName).load().createTempView("sortedTable")

// sort query on sorted data set
scala> val df1 = spark.sql("select * from sortedTable where id % 2 = 0 order by id")

scala> df1.explain
== Physical Plan ==
*(1) Filter (isnotnull(id#0) && ((id#0 % 2) = 0))
+- *(1) DataSourceV2Scan [id#0, negativeid#1], dev.plugins.optimizer.SortedReader@1fcc3461


scala> Seq(("1", 1), ("2", 2), ("3", 3), ("4", 4), ("5", 5)).toDF("value", "id").createTempView("mymemorytable")

// sort query on non-sorted data set
scala> val df2 = spark.sql("select * from mymemorytable where id % 2 = 0 order by id")
 
scala> df2.explain 
== Physical Plan ==
*(2) Sort [id#13 ASC NULLS FIRST], true, 0
+- Exchange rangepartitioning(id#13 ASC NULLS FIRST, 200)
   +- *(1) Project [_1#9 AS value#12, _2#10 AS id#13]
      +- *(1) Filter ((_2#10 % 2) = 0)
         +- LocalTableScan [_1#9, _2#10]

```

## Github 

[Here](https://github.com/falarica/spark-devplugins/tree/master/optimizer)
