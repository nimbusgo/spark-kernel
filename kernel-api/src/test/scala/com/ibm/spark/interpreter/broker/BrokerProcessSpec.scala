package com.ibm.spark.interpreter.broker

import java.io.{OutputStream, InputStream, File}

import org.apache.commons.exec.Executor
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FunSpec, Matchers, OneInstancePerTest}

class BrokerProcessSpec extends FunSpec with Matchers
  with OneInstancePerTest with MockitoSugar
{
  private val TestProcessName = "test_process"
  private val TestEntryResource = "test/entry/resource"
  private val TestOtherResources = Seq("test/resource/1", "test/resource/2")
  private val TestArguments = Seq("a", "b", "c")

  private val mockBrokerBridge = mock[BrokerBridge]
  private val mockBrokerProcessHandler = mock[BrokerProcessHandler]

  private val mockExecutor = mock[Executor]

  private val brokerProcess = new BrokerProcess(
    processName = TestProcessName,
    entryResource = TestEntryResource,
    otherResources = TestOtherResources,
    brokerBridge = mockBrokerBridge,
    brokerProcessHandler = mockBrokerProcessHandler,
    arguments = TestArguments
  ) {
    @volatile private var _tmpDir: String =
      System.getProperty("java.io.tmpdir")

    def setTmpDirectory(newDir: String) = _tmpDir = newDir
    override protected def getTmpDirectory: String = _tmpDir
    override protected def newExecutor(): Executor = mockExecutor
    override protected def copy(
      inputStream: InputStream,
      outputStream: OutputStream
    ): Int = 0

    def doCopyResourceToTmp(resource: String) = copyResourceToTmp(resource)
  }

  describe("BrokerProcess") {
    describe("constructor") {
      it("should fail if the process name is null") {
        intercept[IllegalArgumentException] {
          new BrokerProcess(
            processName = null,
            entryResource = TestEntryResource,
            otherResources = TestOtherResources,
            brokerBridge = mockBrokerBridge,
            brokerProcessHandler = mockBrokerProcessHandler,
            arguments = TestArguments
          )
        }
      }

      it("should fail if the process name is empty") {
        intercept[IllegalArgumentException] {
          new BrokerProcess(
            processName = " \t\n\r",
            entryResource = TestEntryResource,
            otherResources = TestOtherResources,
            brokerBridge = mockBrokerBridge,
            brokerProcessHandler = mockBrokerProcessHandler,
            arguments = TestArguments
          )
        }
      }

      it("should fail if the entry resource is null") {
        intercept[IllegalArgumentException] {
          new BrokerProcess(
            processName = TestProcessName,
            entryResource = null,
            otherResources = TestOtherResources,
            brokerBridge = mockBrokerBridge,
            brokerProcessHandler = mockBrokerProcessHandler,
            arguments = TestArguments
          )
        }
      }

      it("should fail if the entry resource is empty") {
        intercept[IllegalArgumentException] {
          new BrokerProcess(
            processName = TestProcessName,
            entryResource = " \t\n\r",
            otherResources = TestOtherResources,
            brokerBridge = mockBrokerBridge,
            brokerProcessHandler = mockBrokerProcessHandler,
            arguments = TestArguments
          )
        }
      }
    }

    describe("#copyResourceToTmp") {
      it("should fail if a directory with the resource name already exists") {
        val baseDir = System.getProperty("java.io.tmpdir")
        val newResourceName = "some_resource/"

        val resourceFile = new File(baseDir + s"/$newResourceName")
        resourceFile.createNewFile()

        intercept[BrokerException] {
          brokerProcess.doCopyResourceToTmp(resourceFile.getPath)
        }
      }

      it("should throw an exception if the tmp directory is not set") {
        brokerProcess.setTmpDirectory(null)

        intercept[BrokerException] {
          brokerProcess.doCopyResourceToTmp("some file")
        }
      }

      it("should return the resulting destination of the resource") {
        brokerProcess.setTmpDirectory("a")
        val destination = brokerProcess.doCopyResourceToTmp(TestEntryResource)

        destination should be ("a/resource")
      }
    }

    describe("#newProcessEnvironment") {
      it("should return environment variables provided to this JVM") {
        fail()
      }
    }

    describe("#start") {
      it("should throw an exception if the process is already started") {
        fail()
      }

      it("should execute the process using the provided arguments") {
        fail()
      }

      it("should execute using the environment provided") {
        fail()
      }

      it("should use the process handler provided to listen for events") {
        fail()
      }
    }

    describe("#stop") {
      it("should destroy the process if it is running") {
        fail()
      }
    }
  }
}
