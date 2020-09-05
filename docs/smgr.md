## Goal 

Plugin sort shuffle manager of your own with some extra logging.

## Approach

Sort based shuffle as described in Spark's code comments :
In sort-based shuffle, incoming records are sorted according to their target partition ids, then written to a single map output file. Reducers fetch contiguous regions of this file in order to read their portion of the map output.

Shuffle extensively uses network and disk. There are storage specific ways in which one may want to optimize the network and disk i/o done by shuffle. For e.g. Apache Crail have modified Spark's shuffle to write intermediate data to their high performance storage. SortShuffleManager returns ShuffleWriter and ShuffleReader classes that can be implemented to introduce specific behavior. But, this is involved code. For the example here, I will just plugin a sort shuffle manager and a ShuffleWriter that does some extra logging while writing the map records. 

Following things were done in this project: 

* Added a file SortShuffleManagerWithExtraLoggging. This returns our shuffle writer. 
* Implemented ShuffleWriter that does some extra logging. 

## Try it out

Run the ShuffleManagerSpec. 

## Github 

[Here](https://github.com/falarica/spark-devplugins/tree/master/shufflemanager)
