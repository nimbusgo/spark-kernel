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
