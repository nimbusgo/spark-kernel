package com.ibm.spark.interpreter.broker

import com.ibm.spark.interpreter.InterpreterTypes.ExecuteOutput
import com.ibm.spark.interpreter.Results.Result
import com.ibm.spark.interpreter.broker.BrokerTypes.CodeResults
import com.ibm.spark.interpreter.{ExecuteError, ExecuteFailure, Results}

import scala.concurrent.Future

/**
 * Represents a utility that can transform raw broker information to
 * kernel information.
 */
class BrokerTransformer {
  /**
   * Transforms a pure result containing output information into a form that
   * the interpreter interface expects.
   *
   * @param futureResult The raw result as a future
   *
   * @return The transformed result as a future
   */
  def transformToInterpreterResult(futureResult: Future[CodeResults]):
    Future[(Result, Either[ExecuteOutput, ExecuteFailure])] =
  {
    import scala.concurrent.ExecutionContext.Implicits.global

    futureResult
      .map(results => (Results.Success, Left(results)))
      .recover({ case ex: BrokerException =>
        (Results.Error, Right(ExecuteError(
          name = ex.getClass.getName,
          value = ex.getLocalizedMessage,
          stackTrace = ex.getStackTrace.map(_.toString).toList
        )))
      })
  }
}
