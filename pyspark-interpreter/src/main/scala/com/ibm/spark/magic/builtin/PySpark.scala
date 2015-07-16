package com.ibm.spark.magic.builtin

import com.ibm.spark.interpreter.{ExecuteError, ExecuteAborted}
import com.ibm.spark.kernel.interpreter.pyspark.{PySparkInterpreter, PySparkException}
import com.ibm.spark.kernel.protocol.v5.MIMEType
import com.ibm.spark.magic.{CellMagicOutput, CellMagic}
import com.ibm.spark.magic.dependencies.IncludeKernel

/**
 * Represents the magic interface to use the PySpark interpreter.
 */
class PySpark extends CellMagic with IncludeKernel {
  override def execute(code: String): CellMagicOutput = {
    val pySpark = Option(kernel.data.get("PySpark"))

    if (pySpark.isEmpty || pySpark.get == null)
      throw new PySparkException("PySpark is not available!")

    pySpark.get match {
      case pySparkInterpreter: PySparkInterpreter =>
        val (_, output) = pySparkInterpreter.interpret(code)
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

