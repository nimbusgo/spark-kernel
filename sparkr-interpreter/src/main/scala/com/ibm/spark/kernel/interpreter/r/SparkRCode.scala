package com.ibm.spark.kernel.interpreter.r

import com.ibm.spark.kernel.interpreter.r.SparkRTypes._

/**
 * Represents a block of R code to be evaluated.
 *
 * @param codeId The id to associate with the code to be executed
 * @param code The code to evaluate using SparkR
 */
case class SparkRCode(codeId: CodeId, code: Code)

