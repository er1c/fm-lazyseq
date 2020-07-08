/*
 * Copyright (c) 2019 Frugal Mechanic (http://frugalmechanic.com)
 * Copyright (c) 2020 the LazySeq contributors.
 * See the project homepage at: https://er1c.github.io/fm-lazyseq/
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

package fm.lazyseq

import fm.common.Logging
import java.io.Closeable
import scala.collection.mutable.Builder
import java.util.concurrent.BlockingQueue

trait LazySeqBuilderBase[A] extends Builder[A, LazySeq[A]] with Closeable with Logging { self: LazySeqBuilder[A] =>
  protected def abortCheck(): Unit
  protected def closed: Boolean
  protected def closedWarning: Unit
  protected def queue: BlockingQueue[AnyRef]

  final override def addOne(v: A): self.type = {
    abortCheck()

    if (closed) closedWarning
    else if (Thread.interrupted()) throw new InterruptedException
    else queue.put(v.asInstanceOf[AnyRef])

    abortCheck()

    self
  }
}
