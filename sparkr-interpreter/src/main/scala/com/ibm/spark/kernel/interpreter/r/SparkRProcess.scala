package com.ibm.spark.kernel.interpreter.r

import java.io.{File, FileOutputStream}

import org.apache.commons.exec._
import org.apache.commons.exec.environment.EnvironmentUtils
import org.apache.commons.io.IOUtils
import org.slf4j.LoggerFactory

/**
 * Represents the R process used to evaluate SparkR code.
 *
 * @param sparkRBridge The bridge to use to retrieve kernel output streams
 *                      and the Spark version to be verified
 * @param port The port to provide to the SparkR process to use to connect
 *             back to the JVM
 * @param restartOnExit Restarts the process if it exits
 */
class SparkRProcess(
  private val sparkRBridge: SparkRBridge,
  private val port: Int,
  private val restartOnExit: Boolean = true
) {
  private val logger = LoggerFactory.getLogger(this.getClass)

  private val ScriptName = "sparkr_runner.R"
  private val ScriptArgs =
    "--default-packages=datasets,utils,grDevices,graphics,stats,methods"
  private val ScriptDir = "sparkR"
  private val OtherFiles = Seq(
    "backend.R",
    "broadcast.R",
    "client.R",
    "column.R",
    "context.R",
    "DataFrame.R",
    "deserialize.R",
    "generics.R",
    "group.R",
    "jobj.R",
    "pairRDD.R",
    "RDD.R",
    "schema.R",
    "serialize.R",
    "sparkr_runner_utils.R",
    "SQLContext.R",
    "utils.R",
    "zzz.R"
  )

  private val classLoader = this.getClass.getClassLoader
  private val sparkHome = Option(System.getenv("SPARK_HOME"))
    .orElse(Option(System.getProperty("spark.home")))
    .getOrElse("")
  private val processEnvironment = {
    val procEnvironment = EnvironmentUtils.getProcEnvironment

    procEnvironment.put("SPARK_HOME", sparkHome)
    procEnvironment.put("EXISTING_SPARKR_BACKEND_PORT", port.toString)

    procEnvironment
  }

  /** TODO: Allow injection of custom R process. */
  private val rProcess = "Rscript"

  /** Represents the current process being executed. */
  @volatile private var currentExecutor: Option[Executor] = None

  /**
   * Creates a new instance of the SparkR runner script.
   *
   * @return The destination of the SparkR runner script
   */
  protected def newSparkRRunnerScript(): String = {
    def copyFileToTmp(src: String, dir: String = ScriptDir): String = {
      val sparkRRunnerResourceStream =
        classLoader.getResourceAsStream(s"$dir/$src")

      val outputScript =
        new File(System.getProperty("java.io.tmpdir") + s"/$src")

      logger.debug(s"Copying $src to $outputScript")

      // If our script destination is a directory, we cannot copy the script
      if (outputScript.exists() && outputScript.isDirectory)
        throw new SparkRException(s"Failed to create script: $outputScript")

      // Copy the script to the specified temporary destination
      val outputScriptStream = new FileOutputStream(outputScript)
      IOUtils.copy(
        sparkRRunnerResourceStream,
        outputScriptStream
      )
      outputScriptStream.close()

      outputScript.getPath
    }

    // Copy other files
    OtherFiles.foreach(f => copyFileToTmp(f))

    // Copy dist
    //copyFileToTmp("lib", dir = "R")

    // Return the destination of the main script
    copyFileToTmp(ScriptName)
  }

  /**
   * Starts the SparkR process.
   */
  def start(): Unit = currentExecutor.synchronized {
    stop() // Stop any existing process first as we only manage one process

    assert(sparkHome.nonEmpty, "Spark Home must be provided to use SparkR!")

    val script = newSparkRRunnerScript()
    logger.debug(s"New SparkR script created: $script")

    val sparkVersion = sparkRBridge.javaSparkContext.version

    val commandLine = CommandLine.parse(rProcess)
      .addArgument(ScriptArgs)
      .addArgument(script)

    logger.debug(s"SparkR command: ${commandLine.toString}")

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

    logger.debug(s"SparkR environment: $processEnvironment")

    // Start the process using the environment provided to the parent
    executor.execute(commandLine, processEnvironment, new ExecuteResultHandler {
      override def onProcessFailed(ex: ExecuteException): Unit = {
        logger.error(s"SparkR process failed: $ex")
        currentExecutor = None

        sparkRBridge.state.reset()
        if (restartOnExit) start()
      }

      override def onProcessComplete(exitValue: Int): Unit = {
        logger.error(s"SparkR process exited: $exitValue")
        currentExecutor = None

        sparkRBridge.state.reset()
        if (restartOnExit) start()
      }
    })

    currentExecutor = Some(executor)
  }

  /**
   * Stops the SparkR process.
   */
  def stop(): Unit = currentExecutor.synchronized {
    currentExecutor.foreach { executor =>
      logger.debug("Stopping SparkR process")
      executor.getWatchdog.destroyProcess()
    }
    currentExecutor = None
  }
}
