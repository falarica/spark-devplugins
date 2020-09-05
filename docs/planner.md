## Goal 

Modify range operator so that it returns only the multiples of the step 

spark.range(start = 1, end = 10, step = 3, numPartitions = 2).show
    
The actual range would return {1, 4, 7}

The modified range should return {3, 6, 9}
## Approach

Apache Spark's parser generates a abstract syntax tree. The tree nodes are the operators of a query. The AST is passed through various phases of query compilation i.e. analysis, optimizer, planner and execution. In this exercise, I inject a planner rule that replaces a logical plan node with a physical plan node. 

There are multiple ways in which we change the Spark's range function but in this project I will inject a new Spark Plan for the range operator. Steps to achieve this: 

* Add a PlannerExtension that injects our strategy

* Our strategy, ModifiedRangeExecStrategy, replaces the logical range operator with physical plan node ModifiedRangeExec.

* ModifiedRangeExec is a copy of RangeExec in spark except that I have changed the start number of the range. Since RangeExec is a physical plan node, it does the code generation. But let's keep that exercise for some other project.
 
## Try it out

```bash 
$ bin/spark-shell --jars <PATH>/planner-0.1.0-SNAPSHOT.jar --conf spark.sql.extensions=dev.plugins.planner.PlannerExtension

scala> spark.range(start = 1, end = 10, step = 3, numPartitions = 2).show
+---+ 
| id|
+---+
|  3|
|  6|
|  9|
+---+

scala> spark.range(start = 2, end = 10, step = 3, numPartitions = 2).show 
+---+
| id|
+---+
|  3|
|  6|
|  9|
+---+
 

```

## Github 

[Here](https://github.com/falarica/spark-devplugins/tree/master/planner)
