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

import com.ibm.spark.interpreter.{ExecuteError, Results}
import org.scalatest.concurrent.Eventually
import org.scalatest.{OneInstancePerTest, Matchers, FunSpec}

import scala.concurrent.promise

class BrokerTransformerSpec extends FunSpec with Matchers
  with OneInstancePerTest with Eventually
{
  private val brokerTransformer = new BrokerTransformer

  describe("BrokerTransformer") {
    describe("#transformToInterpreterResult") {
      it("should convert to success with result output if no failure") {
        val codeResultPromise = promise[BrokerTypes.CodeResults]()

        val transformedFuture = brokerTransformer.transformToInterpreterResult(
          codeResultPromise.future
        )

        val successOutput = "some success"
        codeResultPromise.success(successOutput)

        eventually {
          val result = transformedFuture.value.get.get
          result should be((Results.Success, Left(successOutput)))
        }
      }

      it("should convert to error with broker exception if failure") {
        val codeResultPromise = promise[BrokerTypes.CodeResults]()

        val transformedFuture = brokerTransformer.transformToInterpreterResult(
          codeResultPromise.future
        )

        val failureException = new BrokerException("some failure")
        codeResultPromise.failure(failureException)

        eventually {
          val result = transformedFuture.value.get.get
          result should be((Results.Error, Right(ExecuteError(
            name = failureException.getClass.getName,
            value = failureException.getLocalizedMessage,
            stackTrace = failureException.getStackTrace.map(_.toString).toList
          ))))
        }
      }
    }
  }
}
