package com.ibm.spark.kernel.interpreter.r

import java.net.URL

import com.ibm.spark.interpreter.Results.Result
import com.ibm.spark.interpreter._
import com.ibm.spark.kernel.api.KernelLike
import org.apache.spark.SparkContext
import org.slf4j.LoggerFactory

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.tools.nsc.interpreter.{InputStream, OutputStream}

/**
 * Represents an interpreter interface to SparkR. Requires a properly-set
 * SPARK_HOME pointing to a binary distribution (needs packaged SparkR library)
 * and an implementation of R on the path.
 *
 * @param _kernel The kernel API to expose to the SparkR instance
 * @param _sparkContext The Spark context to expose to the SparkR instance
 */
class SparkRInterpreter(
  private val _kernel: KernelLike,
  private val _sparkContext: SparkContext
) extends Interpreter {
  private val logger = LoggerFactory.getLogger(this.getClass)

  /** Represents the bridge used by this interpreter's R instance. */
  private lazy val sparkRBridge = new SparkRBridge(_kernel, _sparkContext)

  /** Represents the interface for R to talk to JVM Spark components. */
  private lazy val rBackend = new ReflectiveRBackend

  /** Represents the process handler used for the SparkR process. */
  private lazy val sparkRProcessHandler: SparkRProcessHandler =
    new SparkRProcessHandler(
      sparkRBridge,
      restartOnFailure = true,
      restartOnCompletion = true
    )

  private lazy val sparkRService = new SparkRService(
    rBackend,
    sparkRBridge,
    sparkRProcessHandler
  )
  private lazy val sparkRTransformer = new SparkRTransformer

  /**
   * Executes the provided code with the option to silence output.
   * @param code The code to execute
   * @param silent Whether or not to execute the code silently (no output)
   * @return The success/failure of the interpretation and the output from the
   *         execution or the failure
   */
  override def interpret(code: String, silent: Boolean):
    (Result, Either[ExecuteOutput, ExecuteFailure]) =
  {
    val futureResult = sparkRTransformer.transformToInterpreterResult(
      sparkRService.submitCode(code)
    )

    Await.result(futureResult, 500.seconds)
  }

  /**
   * Starts the interpreter, initializing any internal state.
   * @return A reference to the interpreter
   */
  override def start(): Interpreter = {
    sparkRService.start()

    this
  }

  /**
   * Stops the interpreter, removing any previous internal state.
   * @return A reference to the interpreter
   */
  override def stop(): Interpreter = {
    sparkRService.stop()

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
