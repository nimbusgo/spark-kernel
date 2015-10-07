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

package com.ibm.spark.interpreter

/**
 * Represents a generic failure in execution.
 */
sealed abstract class ExecuteFailure

/**
 * Represents an error resulting from interpret execution.
 * @param name The name of the error
 * @param value The message provided from the error
 * @param stackTrace The stack trace as a list of strings representing lines
 *                   in the stack trace
 */
case class ExecuteError(
  name: String, value: String, stackTrace: List[String]
) extends ExecuteFailure {
  override def toString: String =
    "Name: " + name + "\n" +
    "Message: " + value + "\n" +
    "StackTrace: " + stackTrace.mkString("\n")
}

// TODO: Replace with object?
/**
 * Represents an aborted execution.
 */
class ExecuteAborted extends ExecuteFailure
