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

import java.io.Closeable
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import fm.common.{Resource, SingleUseResource, TaskRunner}

final private class BufferedLazySeq[A](reader: LazySeq[A], size: Int = 1) extends LazySeq[A] with Closeable {
  private def builder: LazySeqBuilder[A] =
    new LazySeqBuilder[A](size).withProducerThread { growable => reader.foreach { growable += _ } }

  override def foreach[U](f: A => U): Unit = builder.lazySeq.foreach { f }
  override def iterator: LazySeqIterator[A] = builder.lazySeq.iterator
  override def close(): Unit = builder.close()
}

final private class ParallelMapLazySeq[A, B](
  reader: LazySeq[A],
  map: A => B,
  threads: Int = 8,
  inputBuffer: Int = 8,
  resultBuffer: Int = 8)
  extends LazySeq[B] {

  private def builderResource: Resource[LazySeqBuilder[Future[B]]] =
    SingleUseResource { new LazySeqBuilder[Future[B]](resultBuffer) }
  private def taskRunnerResource: Resource[TaskRunner] =
    SingleUseResource { TaskRunner("RR-parMap", threads = threads, queueSize = inputBuffer) }

  override def foreach[U](f: B => U): Unit = {
    // If the producer throws an exception we will populate this so we can re-throw it
    @volatile var exception: Option[Throwable] = None

    try {
      taskRunnerResource.use { taskRunner =>
        builderResource.use { builder =>
          // Our producer thread:
          builder.withProducerThread { growable =>
            try {
              reader.foreach { a: A =>
                val future: Future[B] = taskRunner.submit { map(a) }
                growable += future
              }
            } catch {
              case ex: Throwable =>
                exception = Some(ex) // Capture the exception and re-throw so everything shuts down
                throw ex
            }
          }

          // Our consumer
          builder.lazySeq.foreach { future: Future[B] => f(Await.result(future, Duration.Inf)) }
        }
      }
    } catch {
      case aborted: LazySeqBuilder.AbortedException =>
        exception match {
          case Some(ex) => throw ex
          case None => throw aborted
        }
    }
  }
}
