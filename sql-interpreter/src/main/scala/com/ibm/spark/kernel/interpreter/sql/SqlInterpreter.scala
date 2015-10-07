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
package com.ibm.spark.kernel.interpreter.sql

import java.net.URL

import com.ibm.spark.interpreter.{ExecuteFailure, ExecuteOutput, Interpreter}
import com.ibm.spark.interpreter.Results.Result
import org.apache.spark.sql.SQLContext

import scala.concurrent.duration._
import scala.concurrent.Await
import scala.tools.nsc.interpreter.{OutputStream, InputStream}

/**
 * Represents an interpreter interface to Spark SQL.
 */
class SqlInterpreter(private val sqlContext: SQLContext) extends Interpreter {
  private lazy val sqlService = new SqlService(sqlContext)
  private lazy val sqlTransformer = new SqlTransformer

  /**
   * Executes the provided code with the option to silence output.
   * @param code The code to execute
   * @param silent Whether or not to execute the code silently (no output)
   * @return The success/failure of the interpretation and the output from the
   *         execution or the failure
   */
  override def interpret(code: String, silent: Boolean):
    (Result, Either[ExecuteOutput, ExecuteFailure]) =
  {
    if (!sqlService.isRunning) sqlService.start()

    val futureResult = sqlTransformer.transformToInterpreterResult(
      sqlService.submitCode(code)
    )

    Await.result(futureResult, Duration.Inf)
  }

  /**
   * Starts the interpreter, initializing any internal state.
   * @return A reference to the interpreter
   */
  override def start(): Interpreter = {
    sqlService.start()

    this
  }

  /**
   * Stops the interpreter, removing any previous internal state.
   * @return A reference to the interpreter
   */
  override def stop(): Interpreter = {
    sqlService.stop()

    this
  }

  /**
   * Returns the class loader used by this interpreter.
   *
   * @return The runtime class loader used by this interpreter
   */
  override def classLoader: ClassLoader = this.getClass.getClassLoader

  // Unsupported (but can be invoked)
  override def lastExecutionVariableName: Option[String] = None

  // Unsupported (but can be invoked)
  override def read(variableName: String): Option[AnyRef] = None

  // Unsupported (but can be invoked)
  override def completion(code: String, pos: Int): (Int, List[String]) =
    (pos, Nil)

  // Unsupported
  override def updatePrintStreams(in: InputStream, out: OutputStream, err: OutputStream): Unit = ???

  // Unsupported
  override def classServerURI: String = ???

  // Unsupported
  override def interrupt(): Interpreter = ???

  // Unsupported
  override def bind(variableName: String, typeName: String, value: Any, modifiers: List[String]): Unit = ???

  // Unsupported
  override def addJars(jars: URL*): Unit = ???

  // Unsupported
  override def doQuietly[T](body: => T): T = ???
}
