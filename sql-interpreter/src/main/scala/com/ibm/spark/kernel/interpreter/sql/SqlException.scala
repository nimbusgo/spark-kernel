package com.ibm.spark.kernel.interpreter.sql

import com.ibm.spark.interpreter.broker.BrokerException

/**
 * Represents a generic SQL exception.
 *
 * @param message The message to associate with the exception
 */
class SqlException(message: String) extends BrokerException(message)

