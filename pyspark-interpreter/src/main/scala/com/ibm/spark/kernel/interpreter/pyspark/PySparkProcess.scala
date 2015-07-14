package com.ibm.spark.kernel.interpreter.pyspark

import java.io.{FileOutputStream, File}

import org.apache.commons.exec.environment.EnvironmentUtils
import org.apache.commons.exec._
import org.apache.commons.io.IOUtils
import org.slf4j.LoggerFactory

/**
 * Represents the Python process used to evaluate PySpark code.
 *
 * @param pySparkBridge The bridge to use to retrieve kernel output streams
 *                      and the Spark version to be verified
 * @param port The port to provide to the PySpark process to use to connect
 *             back to the JVM
 */
class PySparkProcess(
  private val pySparkBridge: PySparkBridge,
  private val port: Int
) {
  private val logger = LoggerFactory.getLogger(this.getClass)

  private val ScriptName = "pyspark_runner.py"
  private val classLoader = this.getClass.getClassLoader
  private val sparkHome = Option(System.getenv("SPARK_HOME"))
    .orElse(Option(System.getProperty("spark.home")))
    .getOrElse("")
  private val pythonPath = Option(System.getenv("PYTHONPATH")).getOrElse("")
  private val processEnvironment = {
    val procEnvironment = EnvironmentUtils.getProcEnvironment

    // Ensure that PYTHONPATH has proper pointer to SPARK_HOME
    val newPythonPath =
      (pythonPath.split(java.io.File.pathSeparator) :+ s"$sparkHome/python/")
        .map(_.trim).filter(_.nonEmpty).map(new File(_)).distinct
        .mkString(java.io.File.pathSeparator)

    procEnvironment.put("PYTHONPATH", newPythonPath)

    procEnvironment
  }

  /** TODO: Allow injection of custom Python process. */
  private val pythonProcess = "python"

  /** Represents the current process being executed. */
  @volatile private var currentExecutor: Option[Executor] = None

  /**
   * Creates a new instance of the PySpark runner script.
   *
   * @return The destination of the PySpark runner script
   */
  protected def newPySparkRunnerScript(): String = {
    val pySparkRunnerResourceStream =
      classLoader.getResourceAsStream(ScriptName)

    val outputScript =
      new File(System.getProperty("java.io.tmpdir") + s"/$ScriptName")

    // If our script destination is a directory, we cannot copy the script
    if (outputScript.exists() && outputScript.isDirectory)
      throw new PySparkException(s"Failed to create script: $outputScript")

    // Copy the script to the specified temporary destination
    val outputScriptStream = new FileOutputStream(outputScript)
    IOUtils.copy(
      pySparkRunnerResourceStream,
      outputScriptStream
    )
    outputScriptStream.close()

    // Return the destination of the script
    outputScript.getPath
  }

  /**
   * Starts the PySpark process.
   */
  def start(): Unit = currentExecutor.synchronized {
    stop() // Stop any existing process first as we only manage one process

    assert(sparkHome.nonEmpty, "Spark Home must be provided to use PySpark!")

    val script = newPySparkRunnerScript()
    logger.debug(s"New PySpark script created: $script")

    val sparkVersion = pySparkBridge.javaSparkContext.version

    val commandLine = CommandLine
      .parse(pythonProcess)
      .addArgument(script)
      .addArgument(port.toString)
      .addArgument(sparkVersion)

    logger.debug(s"PySpark command: ${commandLine.toString}")

    val executor = new DefaultExecutor

    // TODO: Figure out how to dynamically update the output stream used
    //       to use kernel.out, kernel.err, and kernel.in
    // NOTE: Currently mapping to standard output/input, which will be caught
    //       by our system and redirected through the kernel to the client
    executor.setStreamHandler(new PumpStreamHandler(
      System.out,
      System.err,
      System.in
    ))

    // Marking exit status of 1 as successful exit
    executor.setExitValue(1)

    // Prevent the runner from being killed due to run time as it is a
    // long-term process
    executor.setWatchdog(new ExecuteWatchdog(ExecuteWatchdog.INFINITE_TIMEOUT))

    logger.debug(s"PySpark environment: $processEnvironment")

    // Start the process using the environment provided to the parent
    executor.execute(commandLine, processEnvironment, new ExecuteResultHandler {
      override def onProcessFailed(ex: ExecuteException): Unit = {
        logger.error(s"PySpark process failed: $ex")
        currentExecutor = None
      }

      override def onProcessComplete(exitValue: Int): Unit = {
        logger.error(s"PySpark process exited: $exitValue")
        currentExecutor = None
      }
    })

    currentExecutor = Some(executor)
  }

  /**
   * Stops the PySpark process.
   */
  def stop(): Unit = currentExecutor.synchronized {
    logger.debug("Stopping PySpark process")
    currentExecutor.foreach(_.getWatchdog.destroyProcess())
    currentExecutor = None
  }
}
