package com.ibm.spark.kernel.interpreter.scala

/**
 * Represents a generic Scala exception.
 *
 * @param message The message to associate with the exception
 */
class ScalaException(message: String) extends Throwable(message)
