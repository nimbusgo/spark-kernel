package com.ibm.spark.kernel.interpreter.pyspark

import com.ibm.spark.kernel.api.KernelLike
import com.ibm.spark.kernel.interpreter.pyspark.PySparkTypes._
import org.apache.spark.SparkContext
import org.slf4j.LoggerFactory
import py4j.GatewayServer

import scala.concurrent.Future

/**
 * Represents the service that provides the high-level interface between the
 * JVM and Python.
 *
 * @param _kernel The kernel API to provide to PySpark
 * @param _sparkContext The SparkContext to provide to PySpark
 */
class PySparkService(
  private val _kernel: KernelLike,
  private val _sparkContext: SparkContext
) {
  private val logger = LoggerFactory.getLogger(this.getClass)

  /** Represents the bridge used by this interpreter's Python instance. */
  private lazy val pySparkBridge = new PySparkBridge(_kernel, _sparkContext)

  /** Represents the Py4j gateway to allow Python to communicate with the JVM. */
  private lazy val gatewayServer = new GatewayServer(pySparkBridge, 0)

  /** Represents the process used to execute Python code via the bridge. */
  private lazy val pythonProcess =
    new PySparkProcess(pySparkBridge, gatewayServer.getListeningPort)

  /** Starts the PySpark service. */
  def start(): Unit = {
    // Start without forking the gateway server (needs to have access to
    // SparkContext in current JVM)
    logger.debug("Starting gateway server")
    gatewayServer.start()

    val port = gatewayServer.getListeningPort
    logger.debug(s"Gateway server running on port $port")

    // Start the Python process used to execute code
    logger.debug("Launching process to execute Python code")
    pythonProcess.start()
  }

  /**
   * Submits code to the PySpark service to be executed and return a result.
   *
   * @param code The code to execute
   *
   * @return The result as a future to eventually return
   */
  def submitCode(code: Code): Future[CodeResults] = {
    pySparkBridge.state.pushCode(code)
  }

  /** Stops the running PySpark service. */
  def stop(): Unit = {
    // Stop the Python process used to execute code
    pythonProcess.stop()

    // Stop the server used as an entrypoint for Python
    gatewayServer.shutdown()
  }
}
