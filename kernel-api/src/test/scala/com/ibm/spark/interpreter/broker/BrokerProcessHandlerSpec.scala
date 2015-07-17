package com.ibm.spark.interpreter.broker

import org.apache.commons.exec.ExecuteException
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FunSpec, Matchers, OneInstancePerTest}
import org.mockito.Mockito._
import org.mockito.Matchers._

class BrokerProcessHandlerSpec extends FunSpec with Matchers
  with OneInstancePerTest with MockitoSugar
{
  private val mockBrokerBridge = mock[BrokerBridge]
  private val brokerProcessHandler = new BrokerProcessHandler(
    mockBrokerBridge,
    restartOnFailure = true,
    restartOnCompletion = true
  )

  describe("BrokerProcessHandler") {
    describe("#onProcessFailed") {
      it("should invoke the reset method") {
        val mockResetMethod = mock[String => Unit]
        brokerProcessHandler.setResetMethod(mockResetMethod)

        brokerProcessHandler.onProcessFailed(mock[ExecuteException])

        verify(mockResetMethod).apply(anyString())
      }

      it("should invoke the restart method if the proper flag is set to true") {
        val mockRestartMethod = mock[() => Unit]
        brokerProcessHandler.setRestartMethod(mockRestartMethod)

        brokerProcessHandler.onProcessFailed(mock[ExecuteException])

        verify(mockRestartMethod).apply()
      }
    }

    describe("#onProcessComplete") {
      it("should invoke the reset method") {
        val mockResetMethod = mock[String => Unit]
        brokerProcessHandler.setResetMethod(mockResetMethod)

        brokerProcessHandler.onProcessComplete(0)

        verify(mockResetMethod).apply(anyString())
      }

      it("should invoke the restart method if the proper flag is set to true") {
        val mockRestartMethod = mock[() => Unit]
        brokerProcessHandler.setRestartMethod(mockRestartMethod)

        brokerProcessHandler.onProcessComplete(0)

        verify(mockRestartMethod).apply()
      }
    }
  }
}
