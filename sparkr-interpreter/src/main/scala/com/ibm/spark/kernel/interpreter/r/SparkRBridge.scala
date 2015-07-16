package com.ibm.spark.kernel.interpreter.r

import com.ibm.spark.kernel.api.KernelLike
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Represents constants for the SparkR bridge.
 */
object SparkRBridge {
  /** Represents the maximum amount of code that can be queued for Python. */
  val MaxQueuedCode = 500

  /** Contains the bridge used by the current R process. */
  @volatile private var _sparkRBridge: Option[SparkRBridge] = None

  /** Allows kernel to set bridge dynamically. */
  private[r] def sparkRBridge_=(newSparkRBridge: SparkRBridge): Unit = {
    _sparkRBridge = Some(newSparkRBridge)
  }

  /** Clears the bridge currently hosted statically. */
  private[r] def reset(): Unit = _sparkRBridge = None

  /** Must be exposed in a static location for RBackend to access. */
  def sparkRBridge: SparkRBridge = {
    assert(_sparkRBridge.nonEmpty, "SparkRBridge has not been initialized!")
    _sparkRBridge.get
  }
}

/**
 * Represents the API available to SparkR to act as the bridge for data
 * between the JVM and R.
 *
 * @param _kernel The kernel API to expose through the bridge
 * @param _sparkContext The SparkContext to expose through the bridge
 */
class SparkRBridge(
  private val _kernel: KernelLike,
  private val _sparkContext: SparkContext
) {
  /**
   * Represents the current state of SparkR.
   */
  val state: SparkRState = new SparkRState(SparkRBridge.MaxQueuedCode)

  /**
   * Represents the context used as one of the main entrypoints into Spark
   * for R.
   */
  val javaSparkContext: JavaSparkContext = new JavaSparkContext(_sparkContext)

  /**
   * Represents the context used as the SQL entrypoint into Spark for R.
   */
  val sqlContext: SQLContext = new SQLContext(_sparkContext)

  /**
   * Represents the kernel API available.
   */
  val kernel: KernelLike = _kernel

  /**
   * Represents the configuration containing the current SparkContext setup.
   */
  val sparkConf: SparkConf = _sparkContext.getConf
}
