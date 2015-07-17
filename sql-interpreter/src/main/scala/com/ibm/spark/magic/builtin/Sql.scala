package com.ibm.spark.magic.builtin

import com.ibm.spark.interpreter.{ExecuteError, ExecuteAborted}
import com.ibm.spark.kernel.interpreter.sql.{SqlInterpreter, SqlException}
import com.ibm.spark.kernel.protocol.v5.MIMEType
import com.ibm.spark.magic.{CellMagicOutput, CellMagic}
import com.ibm.spark.magic.dependencies.IncludeKernel

/**
 * Represents the magic interface to use the SQL interpreter.
 */
class Sql extends CellMagic with IncludeKernel {
  override def execute(code: String): CellMagicOutput = {
    val sparkR = Option(kernel.data.get("SQL"))

    if (sparkR.isEmpty || sparkR.get == null)
      throw new SqlException("SQL is not available!")

    sparkR.get match {
      case sparkRInterpreter: SqlInterpreter =>
        val (_, output) = sparkRInterpreter.interpret(code)
        output match {
          case Left(executeOutput) =>
            CellMagicOutput(MIMEType.PlainText -> executeOutput)
          case Right(executeFailure) => executeFailure match {
            case executeAborted: ExecuteAborted =>
              throw new SqlException("SQL code was aborted!")
            case executeError: ExecuteError =>
              throw new SqlException(executeError.value)
          }
        }
      case otherInterpreter =>
        val className = otherInterpreter.getClass.getName
        throw new SqlException(s"Invalid SQL interpreter: $className")
    }
  }
}

