package com.ibm.spark.kernel.interpreter.pyspark

import java.net.URL

import com.ibm.spark.interpreter.Results.Result
import com.ibm.spark.interpreter._
import com.ibm.spark.kernel.api.KernelLike
import org.apache.spark.SparkContext
import org.slf4j.LoggerFactory
import py4j.GatewayServer

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.tools.nsc.interpreter.{InputStream, OutputStream}

/**
 * Represents an interpreter interface to PySpark. Requires a properly-set
 * SPARK_HOME, PYTHONPATH pointing to Spark's Python source, and py4j installed
 * where it is accessible to the Spark Kernel.
 *
 * @param _kernel The kernel API to expose to the PySpark instance
 * @param _sparkContext The Spark context to expose to the PySpark instance
 */
class PySparkInterpreter(
  private val _kernel: KernelLike,
  private val _sparkContext: SparkContext
) extends Interpreter {
  private val logger = LoggerFactory.getLogger(this.getClass)

  /** Represents the bridge used by this interpreter's Python instance. */
  private lazy val pySparkBridge = new PySparkBridge(_kernel, _sparkContext)

  /** Represents the Py4j gateway to allow Python to communicate with the JVM. */
  private lazy val gatewayServer = new GatewayServer(pySparkBridge, 0)

  /** Represents the process used to execute Python code via the bridge. */
  private lazy val pythonProcess =
    new PySparkProcess(pySparkBridge, gatewayServer.getListeningPort)

  /**
   * Executes the provided code with the option to silence output.
   * @param code The code to execute
   * @param silent Whether or not to execute the code silently (no output)
   * @return The success/failure of the interpretation and the output from the
   *         execution or the failure
   */
  override def interpret(code: String, silent: Boolean): (Result, Either[ExecuteOutput, ExecuteFailure]) = {
    import scala.concurrent.ExecutionContext.Implicits.global

    val futureResult = pySparkBridge.state.pushCode(code)
      .map(results => (Results.Success, Left(results)))
      .recover({ case ex: PySparkException => (Results.Error, Right(ExecuteError(
      name = ex.getClass.getName,
      value = ex.getLocalizedMessage,
      stackTrace = ex.getStackTrace.map(_.toString).toList
    )))})

    Await.result(futureResult, 500.seconds)
  }

  /**
   * Starts the interpreter, initializing any internal state.
   * @return A reference to the interpreter
   */
  override def start(): Interpreter = {
    // Start without forking the gateway server (needs to have access to
    // SparkContext in current JVM)
    logger.debug("Starting gateway server")
    gatewayServer.start()

    val port = gatewayServer.getListeningPort
    logger.debug(s"Gateway server running on port $port")

    // Start the Python process used to execute code
    logger.debug("Launching process to execute Python code")
    pythonProcess.start()

    this
  }

  /**
   * Stops the interpreter, removing any previous internal state.
   * @return A reference to the interpreter
   */
  override def stop(): Interpreter = {
    // Stop the Python process used to execute code
    pythonProcess.stop()

    // Stop the server used as an entrypoint for Python
    gatewayServer.shutdown()

    this
  }

  /**
   * Returns the class loader used by this interpreter.
   *
   * @return The runtime class loader used by this interpreter
   */
  override def classLoader: ClassLoader = this.getClass.getClassLoader

  // Unsupported (but can be invoked)
  override def mostRecentVar: String = ""

  // Unsupported (but can be invoked)
  override def read(variableName: String): Option[AnyRef] = None

  // Unsupported (but can be invoked)
  override def completion(code: String, pos: Int): (Int, List[String]) =
    (pos, Nil)

  // Unsupported
  override def updatePrintStreams(in: InputStream, out: OutputStream, err: OutputStream): Unit = ???

  // Unsupported
  override def classServerURI: String = ???

  // Unsupported
  override def interrupt(): Interpreter = ???

  // Unsupported
  override def bind(variableName: String, typeName: String, value: Any, modifiers: List[String]): Unit = ???

  // Unsupported
  override def addJars(jars: URL*): Unit = ???

  // Unsupported
  override def doQuietly[T](body: => T): T = ???
}
