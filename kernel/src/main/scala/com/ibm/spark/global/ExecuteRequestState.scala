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

package com.ibm.spark.global

import com.ibm.spark.kernel.api.Kernel
import com.ibm.spark.kernel.protocol.v5.KernelMessage

/**
 * Represents the state of the kernel messages being received containing
 * execute requests.
 */
object ExecuteRequestState {
  private var _lastKernelMessage: Option[KernelMessage] = None

  /**
   * Processes the incoming kernel message and updates any associated state.
   *
   * @param kernelMessage The kernel message to process
   */
  def processIncomingKernelMessage(kernelMessage: KernelMessage) =
    _lastKernelMessage = Some(kernelMessage)

  /**
   * Returns the last kernel message funneled through the KernelMessageRelay
   * if any.
   *
   * @return Some KernelMessage instance if the relay has processed one,
   *         otherwise None
   */
  def lastKernelMessage: Option[KernelMessage] = _lastKernelMessage

  /**
   * Resets the state of the ExecuteRequestState to the default.
   */
  def reset() = _lastKernelMessage = None
}
