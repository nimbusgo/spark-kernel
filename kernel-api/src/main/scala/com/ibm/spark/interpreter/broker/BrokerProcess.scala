package com.ibm.spark.interpreter.broker

import java.io.{File, FileOutputStream}

import org.apache.commons.exec._
import org.apache.commons.exec.environment.EnvironmentUtils
import org.apache.commons.io.{FilenameUtils, IOUtils}
import org.slf4j.LoggerFactory
import scala.collection.JavaConverters._

/**
 * Represents the process used to evaluate broker code.
 *
 * @param processName The name of the process to invoke
 * @param entryResource The resource to be copied and fed as the first argument
 *                      to the process
 * @param otherResources Other resources to be included in the same directory
 *                       as the main resource
 * @param brokerBridge The bridge to use to retrieve kernel output streams
 *                      and the Spark version to be verified
 * @param brokerProcessHandler The handler to use when the process fails or
 *                             completes
 * @param arguments The collection of additional arguments to pass to the
 *                  process after the main entrypoint
 */
class BrokerProcess(
  private val processName: String,
  private val entryResource: String,
  private val otherResources: Seq[String],
  private val brokerBridge: BrokerBridge,
  private val brokerProcessHandler: BrokerProcessHandler,
  private val arguments: Seq[String] = Nil
) extends BrokerName {
  private val logger = LoggerFactory.getLogger(this.getClass)
  private val classLoader = this.getClass.getClassLoader

  /** Represents the current process being executed. */
  @volatile private[broker] var currentExecutor: Option[Executor] = None

  /**
   * Copies a file from the kernel resources to the temporary directory.
   *
   * @param resource The resource to copy
   *
   * @return The string path pointing to the resource's destination
   */
  protected def copyResourceToTmp(resource: String): String = {
    val brokerRunnerResourceStream = classLoader.getResourceAsStream(resource)

    val tmpDirectory = Option(System.getProperty("java.io.tmpdir"))
      .getOrElse(throw new BrokerException("java.io.tmpdir is not set!"))
    val outputName = FilenameUtils.getName(resource)

    val outputScript = new File(s"$tmpDirectory/$outputName")

    // If our script destination is a directory, we cannot copy the script
    if (outputScript.exists() && outputScript.isDirectory)
      throw new BrokerException(s"Failed to create script: $outputScript")

    // Copy the script to the specified temporary destination
    val outputScriptStream = new FileOutputStream(outputScript)
    IOUtils.copy(
      brokerRunnerResourceStream,
      outputScriptStream
    )
    outputScriptStream.close()

    // Return the destination of the script
    outputScript.getPath
  }

  /**
   * Creates a new process environment to be used for environment variable
   * retrieval by the new process.
   *
   * @return The map of environment variables and their respective values
   */
  protected def newProcessEnvironment(): Map[String, String] = {
    val procEnvironment = EnvironmentUtils.getProcEnvironment

    procEnvironment.asScala.toMap
  }

  /**
   * Starts the Broker process.
   */
  def start(): Unit = currentExecutor.synchronized {
    val capitalizedBrokerName = brokerName.capitalize
    stop() // Stop any existing process first as we only manage one process

    val script = copyResourceToTmp(entryResource)
    otherResources.foreach(copyResourceToTmp)
    logger.debug(s"New $brokerName script created: $script")
    brokerBridge.javaSparkContext.getSparkHome()

    val commandLine = CommandLine
      .parse(processName)
      .addArgument(script)
    arguments.foreach(commandLine.addArgument)

    logger.debug(s"$capitalizedBrokerName command: ${commandLine.toString}")

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

    val processEnvironment = newProcessEnvironment().asJava
    logger.debug(s"$capitalizedBrokerName environment: $processEnvironment")

    // Start the process using the environment provided to the parent
    executor.execute(commandLine, processEnvironment, brokerProcessHandler)

    currentExecutor = Some(executor)
  }

  /**
   * Stops the Broker process.
   */
  def stop(): Unit = currentExecutor.synchronized {
    currentExecutor.foreach(executor => {
      logger.debug(s"Stopping $brokerName process")
      executor.getWatchdog.destroyProcess()
    })
    currentExecutor = None
  }
}
