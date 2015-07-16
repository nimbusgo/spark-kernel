package com.ibm.spark.magic.builtin

import com.ibm.spark.interpreter.{ExecuteError, ExecuteAborted}
import com.ibm.spark.kernel.interpreter.r.{SparkRInterpreter, SparkRException}
import com.ibm.spark.kernel.protocol.v5.MIMEType
import com.ibm.spark.magic.{CellMagicOutput, CellMagic}
import com.ibm.spark.magic.dependencies.IncludeKernel

/**
 * Represents the magic interface to use the SparkR interpreter.
 */
class SparkR extends CellMagic with IncludeKernel {
  override def execute(code: String): CellMagicOutput = {
    val sparkR = Option(kernel.data.get("SparkR"))

    if (sparkR.isEmpty || sparkR.get == null)
      throw new SparkRException("SparkR is not available!")

    sparkR.get match {
      case sparkRInterpreter: SparkRInterpreter =>
        val (_, output) = sparkRInterpreter.interpret(code)
        output match {
          case Left(executeOutput) =>
            CellMagicOutput(MIMEType.PlainText -> executeOutput)
          case Right(executeFailure) => executeFailure match {
            case executeAborted: ExecuteAborted =>
              throw new SparkRException("SparkR code was aborted!")
            case executeError: ExecuteError =>
              throw new SparkRException(executeError.value)
          }
        }
      case otherInterpreter =>
        val className = otherInterpreter.getClass.getName
        throw new SparkRException(s"Invalid SparkR interpreter: $className")
    }
  }
}
