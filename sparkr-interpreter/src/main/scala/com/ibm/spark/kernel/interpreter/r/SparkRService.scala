package com.ibm.spark.kernel.interpreter.r

import java.util.concurrent.{TimeUnit, Semaphore}

import com.ibm.spark.kernel.api.KernelLike
import com.ibm.spark.kernel.interpreter.r.SparkRTypes.{Code, CodeResults}
import org.apache.spark.SparkContext
import org.slf4j.LoggerFactory

import scala.concurrent.{future, Future}

/**
 * Represents the service that provides the high-level interface between the
 * JVM and R.
 *
 * @param _kernel The kernel API to provide to SparkR
 * @param _sparkContext The SparkContext to provide to SparkR
 */
class SparkRService(
  private val _kernel: KernelLike,
  private val _sparkContext: SparkContext
) {
  private val logger = LoggerFactory.getLogger(this.getClass)

  /** Represents the bridge used by this interpreter's R instance. */
  private lazy val sparkRBridge = new SparkRBridge(_kernel, _sparkContext)

  /** Represents the interface for R to talk to JVM Spark components. */
  private lazy val rBackend = new ReflectiveRBackend
  @volatile private var rBackendPort: Int = -1

  /** Represents the process used to execute R code via the bridge. */
  private lazy val rProcess = new SparkRProcess(sparkRBridge, rBackendPort)

  /** Starts the SparkR service. */
  def start(): Unit = {
    logger.debug("Initializing statically-accessible SparkR bridge")
    SparkRBridge.sparkRBridge = sparkRBridge

    val initialized = new Semaphore(0)
    import scala.concurrent.ExecutionContext.Implicits.global
    val rBackendRun = future {
      logger.debug("Initializing RBackend")
      rBackendPort = rBackend.init()
      logger.debug(s"RBackend running on port $rBackendPort")
      initialized.release()
      logger.debug("Running RBackend")
      rBackend.run()
      logger.debug("RBackend has finished")
    }

    // Wait for backend to start before starting R process to connect
    val backendTimeout =
      sys.env.getOrElse("SPARKR_BACKEND_TIMEOUT", "120").toInt
    if (initialized.tryAcquire(backendTimeout, TimeUnit.SECONDS)) {
      // Start the R process used to execute code
      logger.debug("Launching process to execute R code")
      rProcess.start()
    } else {
      // Unable to initialize, so throw an exception
      throw new SparkRException(s"Unable to initialize R backend in ")
    }
  }

  /**
   * Submits code to the SparkR service to be executed and return a result.
   *
   * @param code The code to execute
   *
   * @return The result as a future to eventually return
   */
  def submitCode(code: Code): Future[CodeResults] = {
    sparkRBridge.state.pushCode(code)
  }

  /** Stops the running SparkR service. */
  def stop(): Unit = {
    // Stop the R process used to execute code
    rProcess.stop()

    // Stop the server used as an entrypoint for R
    rBackend.close()

    // Clear the bridge
    SparkRBridge.reset()
  }
}
