/*
 * Copyright 2015 IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ibm.spark.magic.builtin

import com.ibm.spark.interpreter.{ExecuteError, ExecuteAborted}
import com.ibm.spark.kernel.interpreter.pyspark.{PySparkInterpreter, PySparkException}
import com.ibm.spark.kernel.protocol.v5.MIMEType
import com.ibm.spark.magic.{CellMagicOutput, CellMagic}
import com.ibm.spark.magic.dependencies.{IncludeSparkContext, IncludeKernel}
import com.ibm.spark.utils.ArgumentParsingSupport

/**
 * Represents the magic interface to use the PySpark interpreter.
 */
class PySpark extends CellMagic with IncludeKernel with ArgumentParsingSupport with IncludeSparkContext {
  override def execute(code: String): CellMagicOutput = {

    val splitByNewlines = parseArgs(code, "\n")
    val firstLine = splitByNewlines.head.trim
    val trimmedCode = splitByNewlines.tail.mkString("\n").trim
    val namespaceArgs = parseArgs(firstLine)
    val pySpark = if(namespaceArgs.isEmpty){
      Option(kernel.data.get("PySpark"))
    } else if (namespaceArgs.size == 1) {
      val nameSpace = s"Pyspark-${namespaceArgs.head}"
      val interpreterOpt = Option(kernel.data.get(nameSpace))
      if (interpreterOpt.isDefined) {
        interpreterOpt
      } else {
        val newPysparkInterpreter = new PySparkInterpreter(kernel, sparkContext)
        kernel.data.put(nameSpace, newPysparkInterpreter)
        Option(newPysparkInterpreter)
      }
    } else {
      throw new PySparkException(s"Invalid PySpark cell magic options $namespaceArgs")
    }

    if (pySpark.isEmpty || pySpark.get == null)
      throw new PySparkException("PySpark is not available!")

    pySpark.get match {
      case pySparkInterpreter: PySparkInterpreter =>
        val (_, output) = pySparkInterpreter.interpret(trimmedCode)
        output match {
          case Left(executeOutput) =>
            CellMagicOutput(MIMEType.PlainText -> executeOutput)
          case Right(executeFailure) => executeFailure match {
            case executeAborted: ExecuteAborted =>
              throw new PySparkException("PySpark code was aborted!")
            case executeError: ExecuteError =>
              throw new PySparkException(executeError.value)
          }
        }
      case otherInterpreter =>
        val className = otherInterpreter.getClass.getName
        throw new PySparkException(s"Invalid PySpark interpreter: $className")
    }
  }
}

