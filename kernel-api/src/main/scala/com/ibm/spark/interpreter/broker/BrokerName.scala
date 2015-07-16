package com.ibm.spark.interpreter.broker

/**
 * Represents the interface that associates a name with a broker. Can be
 * overridden to change name of broker in subclassing.
 */
trait BrokerName {
  /** The name of the broker. */
  val brokerName: String = "broker"
}
