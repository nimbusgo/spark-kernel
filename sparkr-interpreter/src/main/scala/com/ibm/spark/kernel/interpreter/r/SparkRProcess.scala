package com.ibm.spark.kernel.interpreter.r

import com.ibm.spark.interpreter.broker.BrokerProcess
import scala.collection.JavaConverters._

/**
 * Represents the R process used to evaluate SparkR code.
 *
 * @param sparkRBridge The bridge to use to retrieve kernel output streams
 *                      and the Spark version to be verified
 * @param sparkRProcessHandler The handler to use when the process fails or
 *                             completes
 * @param port The port to provide to the SparkR process to use to connect
 *             back to the JVM
 */
class SparkRProcess(
  private val sparkRBridge: SparkRBridge,
  private val sparkRProcessHandler: SparkRProcessHandler,
  private val port: Int
) extends BrokerProcess(
  processName = "Rscript",
  entryResource = "sparkR/sparkr_runner.R",
  otherResources = Seq("sparkR/sparkr_runner_utils.R"),
  brokerBridge = sparkRBridge,
  brokerProcessHandler = sparkRProcessHandler,
  arguments = Seq(
    "--default-packages=datasets,utils,grDevices,graphics,stats,methods"
  )
) {
  override val brokerName: String = "SparkR"
  private val sparkHome = Option(System.getenv("SPARK_HOME"))
    .orElse(Option(System.getProperty("spark.home")))

  assert(sparkHome.nonEmpty, "SparkR process requires Spark Home to be set!")

  /**
   * Creates a new process environment to be used for environment variable
   * retrieval by the new process.
   *
   * @return The map of environment variables and their respective values
   */
  override protected def newProcessEnvironment(): Map[String, String] = {
    val baseEnvironment = super.newProcessEnvironment()

    baseEnvironment ++ Map(
      "SPARK_HOME"                    -> sparkHome.get,
      "EXISTING_SPARKR_BACKEND_PORT"  -> port.toString
    )
  }
}
