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
import fm.common.Implicits._
import java.io.Closeable
import java.nio.channels.ClosedByInterruptException
import java.util.concurrent.{ArrayBlockingQueue, BlockingQueue, CountDownLatch, SynchronousQueue, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import scala.collection.generic.Growable
import scala.collection.mutable.Builder

object LazySeqBuilder {
  final class AbortedException() extends Exception("LazySeqBuilder has been aborted!")

  private[this] val _uniqueIdAtomicInteger: AtomicInteger = new AtomicInteger(0)
  private def nextUniqueId(): Int = _uniqueIdAtomicInteger.incrementAndGet()
}

/**
 * A LazySeq producer/consumer pair that uses a BlockingQueue
 *
 * I think this is Thread-Safe
 */
final class LazySeqBuilder[A](queueSize: Int = 16, shutdownJVMOnUncaughtException: Boolean = false)
  extends LazySeqBuilderBase[A] { builder =>
  import LazySeqBuilder.AbortedException
  private[this] val END_OF_QUEUE: AnyRef = new Object {}

  protected val uniqueId: Int = LazySeqBuilder.nextUniqueId()

  private class Aborted extends Throwable

  protected[this] val queue: BlockingQueue[AnyRef] =
    if (queueSize > 0) new ArrayBlockingQueue[AnyRef](queueSize) else new SynchronousQueue[AnyRef]()

  def result(): LazySeq[A] = lazySeq

  def clear(): Unit = ???

  protected[this] lazy val closedWarning: Unit = {
    logger.error(
      "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    logger.error("Trying to add to closed LazySeqBuilder.  Unless the app is abnormally terminating, this is an error!")
    logger.error(
      "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
  }

  @volatile protected[this] var closed = false
  @volatile private[this] var aborting = false
  @volatile private[this] var producerThread: ProducerThread = null
  @volatile private[this] var consumerThread: ConsumerThread = null

  private[this] val closingOrAbortingProducer = new AtomicBoolean(false)
  private[this] val closingOrAbortingConsumer = new AtomicBoolean(false)

  private[this] val producerThreadCreated = new AtomicBoolean(false)
  private[this] val consumerThreadCreated = new AtomicBoolean(false)

  protected final def isProducerOrConsumerThread(): Boolean = {
    val currentThread: Thread = Thread.currentThread()
    currentThread === producerThread || currentThread === consumerThread
  }

  protected[this] final def abortCheck(): Unit = {
    if (aborting) {
      if (isProducerOrConsumerThread()) throw new Aborted()
      else throw new AbortedException()
    }
  }

  /**
   * Note: It's not clear what the correct behavior of this method is.
   *       It will currently block until the producer/consumer threads
   *       have cleanly finished.
   */
  def close(): Unit = {
    logger.debug(s"${uniqueId} close() ...")
    closeProducer()
    closeConsumer()
    logger.debug(s"${uniqueId} close() ... DONE!")
  }

  /**
   * This will abort both the producer and consumer possibly removing
   * items from the queue to make room for our END_OF_QUEUE marker.
   */
  def abort(): Unit = {
    logger.debug(s"${uniqueId} abort() ...")
    abortProducer()
    abortConsumer()
    logger.debug(s"${uniqueId} abort() ... DONE!")
  }

  def closeProducer(): Unit = {
    // Only call closeProducer()/abortProducer() once
    if (!closingOrAbortingProducer.compareAndSet(false, true)) return

    logger.debug(s"${uniqueId} closeProducer() ...")

    if (!closed) {
      logger.debug(s"${uniqueId} closeProducer() - queue.put(END_OF_QUEUE) ...")
      queue.put(END_OF_QUEUE)
      logger.debug(s"${uniqueId} closeProducer() - queue.put(END_OF_QUEUE) ... SUCCESS!")
      closed = true
    }

    val pt: ProducerThread = producerThread

    if (null != pt && Thread.currentThread() != pt) {
      logger.debug(s"${uniqueId} closeProducer() - Waiting for ProducerThread to finish ...")
      pt.latch.await()
      logger.debug(s"${uniqueId} closeProducer() - Waiting for ProducerThread to finish ... DONE!")
    }

    // Clear the reference to the producer thread
    producerThread = null
    logger.debug(s"${uniqueId} closeProducer() ... DONE!")
  }

  def abortProducer(): Unit = {
    // Only call closeProducer()/abortProducer() once
    if (!closingOrAbortingProducer.compareAndSet(false, true)) return

    logger.debug(s"${uniqueId} abortProducer() ...")

    aborting = true

    if (!closed) {
      if (queue.isInstanceOf[SynchronousQueue[A]]) {
        // If this is a SynchronousQueue (which as a capacity of 0) then we need to make sure we have a timeout
        // otherwise we could be trying to insert forever
        queue.offer(END_OF_QUEUE, 1000, TimeUnit.MILLISECONDS)
      } else {
        // Remove items from the queue until we have room to add END_OF_QUEUE to it
        while (!queue.offer(END_OF_QUEUE)) queue.poll()
      }

      closed = true
    }

    val pt: ProducerThread = producerThread
    if (null != pt && Thread.currentThread() != pt) {
      // Interrupt the producer thread
      pt.interrupt()

      logger.debug(s"${uniqueId} abortProducer() - Waiting for ProducerThread to finish ...")

      while (!pt.latch.await(5, TimeUnit.SECONDS)) {
        logger.warn(
          s"Still Waiting for ProducerThread to finish.  Expected toe pt.interrupt() call to cause it to finish.")
        pt.interrupt()
      }

      logger.debug(s"${uniqueId} abortProducer() - Waiting for ProducerThread to finish ... DONE!")
    }

    // Clear the reference to the producer thread
    producerThread = null

    logger.debug(s"${uniqueId} abortProducer() ... DONE!")
  }

  def closeConsumer(): Unit = {
    // Only call closeConsumer()/abortConsumer() once
    if (!closingOrAbortingConsumer.compareAndSet(false, true)) return

    logger.debug(s"${uniqueId} closeConsumer() ...")

    val ct: ConsumerThread = consumerThread

    if (null != ct && Thread.currentThread() != ct) {
      logger.debug(s"${uniqueId} closeConsumer() - Waiting for the ConsumerThread to finish ...")
      ct.latch.await()
      logger.debug(s"${uniqueId} closeConsumer() - Waiting for the ConsumerThread to finish ... DONE!")
    }

    // Clear the reference to the consumer thread
    consumerThread = null
    logger.debug(s"${uniqueId} closeConsumer() ... DONE!")
  }

  def abortConsumer(): Unit = {
    // Only call closeConsumer()/abortConsumer() once
    if (!closingOrAbortingConsumer.compareAndSet(false, true)) return

    logger.debug(s"${uniqueId} abortConsumer() ...")

    aborting = true

    val ct: ConsumerThread = consumerThread
    if (null != ct && Thread.currentThread() != ct) {
      // Interrupt the consumer thread
      ct.interrupt()

      logger.debug(s"${uniqueId} abortConsumer() - Waiting for the ConsumerThread to finish ...")

      // Wait for the consumer thread to finish
      while (!ct.latch.await(5, TimeUnit.SECONDS)) {
        logger.warn(
          s"${uniqueId} Still Waiting for ConsumerThread to finish.  Expected toe ct.interrupt() call to cause it to finish.")
        ct.interrupt()
      }

      logger.debug(s"${uniqueId} abortConsumer() - Waiting for the ConsumerThread to finish ... DONE!")
    }

    // Clear the reference to the consumer thread
    consumerThread = null

    // If there is anything remaining in the queue lets drain it to unblock any producers
    if (null != queue.poll()) {
      while (null != queue.poll(100, TimeUnit.MILLISECONDS)) {}
    }

    // Since we might have drained the END_OF_QUEUE marker lets offer that back
    // to avoid blocking any consumers.
    queue.offer(END_OF_QUEUE, 100, TimeUnit.MILLISECONDS)

    logger.debug(s"${uniqueId} abortConsumer() ... DONE!")
  }

  object lazySeq extends LazySeq[A] with Closeable { reader =>
    private[this] var hd: AnyRef = null
    private[this] var hdDefined: Boolean = false

    override def head: A = {
      if (!hasNext) throw new NoSuchElementException("No more elements in iterator")
      hd.asInstanceOf[A]
    }

    override def headOption: Option[A] = if (hasNext) Some(head) else None

    def next(): A = {
      if (!hasNext) throw new NoSuchElementException("No more elements in iterator")
      val res: A = hd.asInstanceOf[A]
      hd = null
      hdDefined = false
      res
    }

    def hasNext: Boolean = {
      abortCheck()

      if (!hdDefined) {
        hd = queue.take()
        hdDefined = true
        abortCheck()
      }

      hd ne END_OF_QUEUE
    }

    override def close(): Unit = {
      logger.debug(s"${uniqueId} resourceReader.close() ...")
      builder.abortProducer()
      builder.abortConsumer()
      logger.debug(s"${uniqueId} resourceReader.close() ... DONE!")
    }

    override object iterator extends LazySeqIterator[A] {
      override def hasNext: Boolean = reader.hasNext
      override def head: A = reader.head
      override def next(): A = reader.next()
      override def close(): Unit = reader.close()
    }

    override def foreach[U](f: A => U): Unit =
      try {
        while (hasNext) f(next())
      } catch {
        case ex: Throwable =>
          logger.warn("Caught Exception running LazySeqBuilder.foreach().  Aborting...", ex)
          builder.abort()
          throw ex
      }
  }

  /**
   * Run the function in a separate producer thread.  Should only be called once since
   * it will close the Builder when the thread finishes.
   */
  def withProducerThread(f: Growable[A] => Unit): this.type = {
    require(producerThreadCreated.compareAndSet(false, true), "withProducerThread already called!")

    require(producerThread == null, "producerThread should be null")

    producerThread = new ProducerThread(f)
    producerThread.start()

    this
  }

  def withConsumerThread(f: LazySeq[A] => Unit): this.type = {
    require(consumerThreadCreated.compareAndSet(false, true), "withConsumerThread already called!")

    require(consumerThread == null, "consumerThread should be null")

    consumerThread = new ConsumerThread(f)
    consumerThread.start()

    this
  }

  /**
   * Wait for both the producer and consumer threads (if any) to finish
   */
  def await(): Unit = {
    awaitProducer()
    awaitConsumer()
  }

  /**
   * Wait for the producer thread (if any) to finish
   */
  def awaitProducer(): Unit = {
    logger.debug(s"${uniqueId} awaitProducer() ...")
    val pt: ProducerThread = producerThread
    if (null != pt) {
      require(pt != Thread.currentThread(), "awaitProducer() - Can't wait on our own thread!")
      pt.latch.await()
    }
    logger.debug(s"${uniqueId} awaitProducer() ... DONE!")
  }

  /**
   * Wait for the consumer thread (if any) to finish
   */
  def awaitConsumer(): Unit = {
    logger.debug(s"${uniqueId} awaitConsumer() ...")
    val ct: ConsumerThread = consumerThread
    if (null != ct) {
      require(ct != Thread.currentThread(), "awaitConsumer() - Can't wait on our own thread!")
      ct.latch.await()
    }
    logger.debug(s"${uniqueId} awaitConsumer() ... DONE!")
  }

  private class ProducerThread(f: Growable[A] => Unit) extends Thread(s"RR-Builder-Producer-${uniqueId}") {
    setDaemon(true)
    val latch: CountDownLatch = new CountDownLatch(1)

    override def run(): Unit =
      try {
        if (closed || aborting) return

        logger.debug(s"${uniqueId} ProducerThread.run() ...")
        f(builder)
        logger.debug(
          s"${uniqueId} ProducerThread.run() finished running f(builder), now waiting for close() to complete...")
        builder.close()
        logger.debug(s"${uniqueId} ProducerThread.run() ... DONE!")
      } catch {
        case _: Aborted => // Expected if we are aborting
        case _: InterruptedException => // Expected if we are aborting
        case _: ClosedByInterruptException => // Expected if we are aborting
        case ex: Throwable =>
          logger.warn("ProducerThread Caught Throwable - Aborting Consumer ...", ex)
          builder.abort()
          throw ex
      } finally {
        latch.countDown()
      }

    // This shouldn't be called, but it's here just in case
    setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler {
      def uncaughtException(t: Thread, e: Throwable): Unit = {
        logger.error("Uncaught Exception", e)
        if (shutdownJVMOnUncaughtException) {
          logger.error("Shutting down JVM")
          Runtime.getRuntime.exit(-1)
        }
      }
    })
  }

  private class ConsumerThread(f: LazySeq[A] => Unit) extends Thread(s"RR-Builder-Consumer-${uniqueId}") {
    setDaemon(true)
    val latch: CountDownLatch = new CountDownLatch(1)

    override def run(): Unit =
      try {
        if (aborting) return

        logger.debug(s"${uniqueId} ConsumerThread.run() ...")
        f(lazySeq)
        logger.debug(s"${uniqueId} ConsumerThread.run() ... DONE!")
      } catch {
        case _: Aborted => // Expected if we are aborting
        case _: InterruptedException => // Expected if we are aborting
        case _: ClosedByInterruptException => // Expected if we are aborting
        case ex: Throwable =>
          logger.warn("ConsumerThread Caught Throwable - Aborting Producer ...", ex)
          builder.abort()
          throw ex
      } finally {
        latch.countDown()
      }

    // This shouldn't be called, but it's here just in case
    setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler {
      def uncaughtException(t: Thread, e: Throwable): Unit = {
        logger.error("Uncaught Exception", e)
        if (shutdownJVMOnUncaughtException) {
          logger.error("Shutting down JVM")
          Runtime.getRuntime.exit(-1)
        }
      }
    })
  }

}
