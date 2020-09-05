package org.apache.spark.sql.dev.plugins

import java.io.{BufferedReader, InputStreamReader}

import org.apache.spark.SparkContext
import org.apache.spark.rpc.RpcEndpointAddress
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend

/**
  * Ideally cluster manager is a separate process, which is responsible for launching
  * executors when driver requests. But here, driver process itself is launching
  * the executor process.
  *
  * @author Hemant Bhanawat
  */
class ModifiedCoarseGrainedSchedulerBackend(sc: SparkContext,
    scheduler: ModifiedTaskSchedulerImpl, maxCores: Int, val executorInstances: Int)
  extends CoarseGrainedSchedulerBackend(scheduler, sc.env.rpcEnv) {

  /*
     After Driver component starts,  it also launches executors using CoarseGrainedExecutorBackend
     class
   */
  override def start() {
    super.start()
    // The endpoint for executors to talk to us
    val driverUrl = RpcEndpointAddress(
      sc.conf.get("spark.driver.host"),
      sc.conf.get("spark.driver.port").toInt,
      CoarseGrainedSchedulerBackend.ENDPOINT_NAME).toString
    for (i <- 0 until executorInstances) {
      val args = Seq("java", "-cp", "/install/spark-2.3.1-bin-hadoop2.7/jars/*:/home/hemant/repos/spark-devplugins/clustermanager/target/clustermanager-0.1.0-SNAPSHOT.jar",
        "org.apache.spark.executor.CoarseGrainedExecutorBackend",
        "--driver-url", driverUrl,
        "--executor-id", i.toString,
        "--hostname", "localhost",
        "--cores", maxCores.toString,
        "--app-id", "myapp")
      val p = new ProcessBuilder()
      p.redirectErrorStream(true)
      import collection.JavaConversions._
      p.command(args.toList)
      val proc = p.start()
      val is = new BufferedReader(new InputStreamReader(proc.getErrorStream))
      val line = is.readLine
      if (line != null) System.out.println(line)

      val is1 = new BufferedReader(new InputStreamReader(proc.getInputStream))
      val line1 = is1.readLine
      if (line1 != null) System.out.println(line1)
    }

  }


}
