package com.ibm.spark.kernel.interpreter.pyspark

import com.ibm.spark.interpreter.{Results, ExecuteError, ExecuteFailure}
import com.ibm.spark.interpreter.InterpreterTypes.ExecuteOutput
import com.ibm.spark.interpreter.Results.Result

import scala.concurrent.Future

import com.ibm.spark.kernel.interpreter.pyspark.PySparkTypes._

/**
 * Represents a utility that can transform raw PySpark information to
 * kernel information.
 */
class PySparkTransformer {
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
      .recover({ case ex: PySparkException =>
        (Results.Error, Right(ExecuteError(
          name = ex.getClass.getName,
          value = ex.getLocalizedMessage,
          stackTrace = ex.getStackTrace.map(_.toString).toList
        )))
      })
  }
}
