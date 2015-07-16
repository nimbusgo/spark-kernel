package com.ibm.spark.interpreter.broker

import com.ibm.spark.interpreter.broker.BrokerTypes.{CodeResults, CodeId}

import scala.concurrent.Promise

/**
 * Represents a promise made regarding the completion of broker code execution.
 *
 * @param codeId The id of the code that was executed
 * @param promise The promise to be fulfilled when the code finishes executing
 */
case class BrokerPromise(codeId: CodeId, promise: Promise[CodeResults])
