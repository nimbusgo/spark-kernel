package com.ibm.spark.kernel.interpreter.sql

import java.io.ByteArrayOutputStream

import com.ibm.spark.interpreter.broker.BrokerService
import com.ibm.spark.kernel.interpreter.sql.SqlTypes._
import org.apache.spark.sql.SQLContext

import scala.concurrent.{Future, future}

/**
 * Represents the service that provides the high-level interface between the
 * JVM and Spark SQL.
 *
 * @param sqlContext The SQL Context of Apache Spark to use to perform SQL
 *                   queries
 */
class SqlService(private val sqlContext: SQLContext) extends BrokerService {
  import scala.concurrent.ExecutionContext.Implicits.global

  @volatile private var _isRunning: Boolean = false
  override def isRunning: Boolean = _isRunning

  /**
   * Submits code to the broker service to be executed and return a result.
   *
   * @param code The code to execute
   *
   * @return The result as a future to eventually return
   */
  override def submitCode(code: Code): Future[CodeResults] = future {
    println(s"Executing: '${code.trim}'")
    val result = sqlContext.sql(code.trim)

    // TODO: There is an internal method used for show called showString that
    //       supposedly is only for the Python API, look into why
    val stringOutput = {
      val outputStream = new ByteArrayOutputStream()
      Console.withOut(outputStream) {
        // TODO: Provide some way to change the number of records shown
        result.show(10)
      }
      outputStream.toString("UTF-8")
    }

    stringOutput
  }

  /** Stops the running broker service. */
  override def stop(): Unit = _isRunning = false

  /** Starts the broker service. */
  override def start(): Unit = _isRunning = true
}
