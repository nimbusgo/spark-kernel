package com.ibm.spark.kernel.interpreter.r

/**
 * Represents a generic SparkR exception.
 *
 * @param message The message to associate with the exception
 */
class SparkRException(message: String) extends Throwable(message)

