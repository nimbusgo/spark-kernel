package com.ibm.spark.interpreter.broker

/**
 * Represents a generic broker exception.
 *
 * @param message The message to associate with the exception
 */
class BrokerException(message: String) extends Throwable(message)

