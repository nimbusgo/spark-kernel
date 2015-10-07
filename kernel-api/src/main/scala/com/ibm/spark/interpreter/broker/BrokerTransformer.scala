/*
 * Copyright 2015 IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
