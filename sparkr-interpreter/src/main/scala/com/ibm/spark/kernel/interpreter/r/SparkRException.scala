package com.ibm.spark.kernel.interpreter.r

import com.ibm.spark.interpreter.broker.BrokerException

/**
 * Represents a generic SparkR exception.
 *
 * @param message The message to associate with the exception
 */
class SparkRException(message: String) extends BrokerException(message)
