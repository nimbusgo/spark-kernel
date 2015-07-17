package com.ibm.spark.interpreter.broker

import com.ibm.spark.kernel.api.KernelLike
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FunSpec, Matchers, OneInstancePerTest}
import org.mockito.Mockito._

class BrokerBridgeSpec extends FunSpec with Matchers with OneInstancePerTest
  with MockitoSugar
{
  private val mockBrokerState = mock[BrokerState]
  private val mockKernel = mock[KernelLike]
  private val mockSparkConf = mock[SparkConf]
  private val mockSparkContext = mock[SparkContext]

  // A new SQLContext is created per request, meaning this needs mocking
  doReturn(mockSparkConf).when(mockSparkContext).getConf
  doReturn(Array[(String, String)]()).when(mockSparkConf).getAll

  private val brokerBridge = new BrokerBridge(
    mockBrokerState,
    mockKernel,
    mockSparkContext
  )

  describe("BrokerBridge") {
    describe("#state") {
      it("should return the broker state from the constructor") {
        brokerBridge.state should be (mockBrokerState)
      }
    }

    describe("#javaSparkContext") {
      it("should return a JavaSparkContext wrapping the SparkContext") {
        brokerBridge.javaSparkContext.sc should be (mockSparkContext)
      }
    }

    describe("#sqlContext") {
      it("should return a SQLContext wrapping the SparkContext") {
        brokerBridge.sqlContext.sparkContext should be (mockSparkContext)
      }
    }

    describe("#kernel") {
      it("should return the kernel from the constructor") {
        brokerBridge.kernel should be (mockKernel)
      }
    }

    describe("#sparkConf") {
      it("should return the configuration from the SparkContext") {
        brokerBridge.sparkConf should be (mockSparkConf)
      }
    }
  }
}
