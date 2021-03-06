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

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

final class TestLazySeqBuilder extends AnyFunSuite with Matchers {
  private trait TestObj
  private case object Foo extends TestObj
  private case object Bar extends TestObj
  private case object Baz extends TestObj

  private def newBuilder(queueSize: Int = 1): LazySeqBuilder[TestObj] =
    new LazySeqBuilder[TestObj](queueSize = queueSize, shutdownJVMOnUncaughtException = false)

  test("Single Threaded - Close") {
    val builder = newBuilder()
    builder += Foo
    builder.lazySeq.next() should equal(Foo)
    builder.close()
    builder.lazySeq.hasNext should equal(false)
  }

  test("Single Threaded - Abort") {
    val builder = newBuilder()
    builder += Foo
    builder.abort()
  }

  test("withProducerThread - Close") {
    val builder = newBuilder()
    builder.withProducerThread { growable =>
      growable += Foo
      growable += Bar
    }

    builder.lazySeq.next() should equal(Foo)
    builder.lazySeq.next() should equal(Bar)
    builder.close()
    builder.lazySeq.hasNext should equal(false)
  }

  test("withProducerThread - Abort") {
    val builder = newBuilder()
    builder.withProducerThread { growable =>
      growable += Foo
      growable += Bar
    }

    builder.abort()
  }

  test("withConsumerThread - Close") {
    val builder = newBuilder()
    builder.withConsumerThread { reader =>
      reader.toIndexedSeq should equal(IndexedSeq(Foo, Bar))
    }

    builder += Foo
    builder += Bar
    builder.close()
  }

  test("withConsumerThread - Abort Clean") {
    val builder = newBuilder()
    builder.withConsumerThread { reader =>
      reader.toIndexedSeq should equal(IndexedSeq(Foo, Bar))
    }

    builder += Foo
    builder += Bar
    builder.abort()
  }

  test("withConsumerThread - Abort Unclean") {
    val builder = newBuilder()
    @volatile var failed: Boolean = false
    builder.withConsumerThread { reader =>
      reader.toIndexedSeq should equal(IndexedSeq(Foo, Bar))
      failed = true
    }

    builder += Foo
    builder += Bar
    builder.abort()
    builder.awaitConsumer()

    failed should equal(false)
  }

  test("withProducerThread - Exception") {
    val builder = newBuilder()
    builder.withProducerThread { growable =>
      growable += Foo
      growable += Bar
      throw new Throwable("Uncaught Throwable from withProducerThread")
    }

    try {
      // This will either work or throw an AbortedException depending on timing.
      builder.lazySeq.toIndexedSeq should equal(IndexedSeq(Foo, Bar))
    } catch {
      case ex: LazySeqBuilder.AbortedException => // This is also okay
    }

  }

  test("withConsumerThread - Exception") {
    val builder = newBuilder()

    builder.withConsumerThread { reader =>
      builder.lazySeq.next() should equal(Foo)
      builder.lazySeq.next() should equal(Bar)
      throw new Throwable("Uncaught Throwable from withConsumerThread")
    }

    builder += Foo
    builder += Bar

    intercept[LazySeqBuilder.AbortedException] {
      (1 to 1000).foreach { i =>
        builder += Foo
        builder += Bar
        builder += Baz
      }
    }
  }

  test("withProducerThread/withConsumerThread - delayed consumer") {
    val builder = newBuilder(queueSize = 2)
    builder.withProducerThread { growable =>
      growable += Foo
    }

    builder.awaitProducer()

    @volatile var result: List[TestObj] = Nil

    builder.withConsumerThread { reader =>
      result = reader.toList
    }

    builder.awaitConsumer()

    result should equal(List(Foo))
  }

  test("LazySeqBuilder foreach with exception") {
    val builder = newBuilder()

    builder.withProducerThread { growable =>
      growable += Foo
      growable += Bar
      growable += Baz
    }

    intercept[Exception] {
      builder.result().foreach { obj =>
        throw new Exception("foo")
      }
    }

    builder.awaitProducer()
  }

  test("LazySeqBuilder synchronous queue - exception") {
    val builder = newBuilder(queueSize = 0)

    builder.withProducerThread { growable =>
      growable += Foo
      growable += Bar
      growable += Baz
    }

    intercept[Exception] {
      builder.result().foreach { obj =>
        throw new Exception("foo")
      }
    }

    builder.awaitProducer()
  }
}
