package com.ibm.spark.interpreter.broker

import BrokerTypes._

/**
 * Represents a block of code to be evaluated.
 *
 * @param codeId The id to associate with the code to be executed
 * @param code The code to evaluate using the broker
 */
case class BrokerCode(codeId: CodeId, code: Code)

