package com.ibm.spark.kernel.interpreter.pyspark

/**
 * Represents a generic PySpark exception.
 *
 * @param message The message to associate with the exception
 */
class PySparkException(message: String) extends Throwable(message)

