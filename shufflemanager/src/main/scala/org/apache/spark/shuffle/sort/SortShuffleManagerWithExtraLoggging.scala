package org.apache.spark.shuffle.sort

import org.apache.spark.shuffle.{BaseShuffleHandle, ShuffleHandle, ShuffleWriter}
import org.apache.spark.{SparkConf, TaskContext}

class SortShuffleManagerWithExtraLoggging(conf: SparkConf)  extends SortShuffleManager(conf) {

  /** Get a writer for a given partition. Called on executors by map tasks. */
  override def getWriter[K, V](
                                handle: ShuffleHandle,
                                mapId: Int,
                                context: TaskContext): ShuffleWriter[K, V] = {
      // spark-devPlugin- This is inefficient because we are not allowing serialized sort
      // shuffle and bypass merge sort. But this is only for demonstration
      return new SortShuffleWriterWithExtraLogging(shuffleBlockResolver,
          handle.asInstanceOf[BaseShuffleHandle[K@unchecked, V@unchecked, _]], mapId, context)
  }

}
