## Goal 

For a specific datasource, add an extra column that is derived from two other columns. 

Such a scenario can arise if you cannot change the datasource but you want to avoid manually adding that column every time. 

## Approach

Apache Spark's parser generates a abstract syntax tree. The tree nodes are the operators of a query. The AST is passed through various phases of query compilation i.e. resolution, analysis, optimizer, planner and execution. In this exercise, we inject a resolver rule. This resolver rule adds a new column total salary based on the salary and bonus that are two other columns in the data source. 

* ResolverExtension injects the rule AddMonthColumn
* The rule AddMonthColumn inserts a Project node in the existing plan tree with the new column. 


## Try it out

```bash 
$ bin/spark-shell --jars <PATH>/resolver-0.1.0-SNAPSHOT.jar  --conf spark.sql.extensions=dev.plugins.resolver.ResolverExtension 


scala> spark.sql("create table salary (id string, salary integer, bonus integer) using parquet options (path '/tmp/p')")
res0: org.apache.spark.sql.DataFrame = []

scala> spark.sql("Insert Into salary values ('1', 100000, 10000)")

scala> spark.sql("Insert Into salary values ('2', 200000, 15000)")

scala> spark.sql("select * from salary").show
+---+------+-----+----------------------+
| id|salary|bonus|syscol_yearly_takeaway|
+---+------+-----+----------------------+
|  1|100000|10000|                110000|
|  2|200000|15000|                215000|
+---+------+-----+----------------------+

```

## Github 

[Here](https://github.com/falarica/spark-devplugins/tree/master/resolver)
