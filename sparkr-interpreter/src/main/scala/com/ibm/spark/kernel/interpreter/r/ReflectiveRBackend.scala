package com.ibm.spark.kernel.interpreter.r

/**
 * Provides reflective access into the backend R component that is not
 * publically accessible.
 */
class ReflectiveRBackend {
  private val rBackendClass = Class.forName("org.apache.spark.api.r.RBackend")
  private val rBackendInstance = rBackendClass.newInstance()

  /**
   * Initializes the underlying RBackend service.
   *
   * @return The port used by the service
   */
  def init(): Int = {
    val runMethod = rBackendClass.getDeclaredMethod("init")

    runMethod.invoke(rBackendInstance).asInstanceOf[Int]
  }

  /** Blocks until the service has finished. */
  def run(): Unit = {
    val runMethod = rBackendClass.getDeclaredMethod("run")

    runMethod.invoke(rBackendInstance)
  }

  /** Closes the underlying RBackend service. */
  def close(): Unit = {
    val runMethod = rBackendClass.getDeclaredMethod("close")

    runMethod.invoke(rBackendInstance)
  }
}
