package com.ibm.spark.kernel.interpreter.pyspark

import com.ibm.spark.interpreter.broker.BrokerException

/**
 * Represents a generic PySpark exception.
 *
 * @param message The message to associate with the exception
 */
class PySparkException(message: String) extends BrokerException(message)

