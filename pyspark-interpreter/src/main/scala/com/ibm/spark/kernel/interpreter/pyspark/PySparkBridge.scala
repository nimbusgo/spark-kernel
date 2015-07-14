package com.ibm.spark.kernel.interpreter.pyspark

import com.ibm.spark.kernel.api.KernelLike
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Represents constants for the PySpark bridge.
 */
object PySparkBridge {
  /** Represents the maximum amount of code that can be queued for Python. */
  val MaxQueuedCode = 500
}

/**
 * Represents the API available to PySpark to act as the bridge for data
 * between the JVM and Python.
 *
 * @param _kernel The kernel API to expose through the bridge
 * @param _sparkContext The SparkContext to expose through the bridge
 */
class PySparkBridge(
  private val _kernel: KernelLike,
  private val _sparkContext: SparkContext
) {
  /**
   * Represents the current state of PySpark.
   */
  val state: PySparkState = new PySparkState(PySparkBridge.MaxQueuedCode)

  /**
   * Represents the context used as one of the main entrypoints into Spark
   * for Python.
   */
  val javaSparkContext: JavaSparkContext = new JavaSparkContext(_sparkContext)

  /**
   * Represents the context used as the SQL entrypoint into Spark for Python.
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
