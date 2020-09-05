## Goal 

There are two goals: 

* For a specific data source, allow queries that have an equality check on the "id" column. You may want to do this when your datasource is highly optimized for point queries. 

* Disallow all types of joins except INNER joins. You may want to do this for your larger tables. 

## Approach

Apache Spark's parser generates an abstract syntax tree. The tree nodes are the operators of a query. The AST is passed through various phases of query compilation i.e. analysis, optimizer, planner and execution. In this exercise, we inject a check rule in the analyzer. The check rule throws an exception if there is no where clause with an equality check for a parquet data source. It also throws an exception if there is a join of non-inner type. This check rule is applied by the Spark during the analysis phase. 

* EqualityCheckSessionExtension injects the check rule

* EqualityCheckExtension has the code to analyze the plan and verify the filter clause and the join clause


## Try it out

```bash 
$ bin/spark-shell --jars <PATH>/analyzer-0.1.0-SNAPSHOT.jar --conf spark.sql.extensions=dev.plugins.analyzer.EqualityCheckSessionExtension 

scala> spark.sql("create table myidtable (value string, id integer) using parquet options (path '/tmp/p')")

scala> spark.sql("Insert Into myidtable values ('1', 1)")
scala> spark.sql("Insert Into myidtable values ('2', 2)")

scala> spark.sql("select * from myidtable a  left  join myidtable b on a.id = b.id and a.id =1")
<Exception>

scala> spark.sql("select * from myidtable")
<Exception>

scala> spark.sql("select * from myidtable a, myidtable b " +
        "where a.id = b.id and a.value = b.value and a.id = 1").collect.length  
res4: Int = 1 

```

## Github 

[Here](https://github.com/falarica/spark-devplugins/tree/master/analyzer)
