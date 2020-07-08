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

import fm.common._
import scala.collection.{BuildFrom, IterableOnce, IterableOnceOps}
import scala.collection.{Factory, IterableFactory, IterableOps}
import scala.collection.WithFilter
import scala.collection.mutable.Builder
import scala.annotation.unchecked.uncheckedVariance

trait LazySeqObjBase[CC[A] <: IterableOps[A, LazySeq, LazySeq[A]]] extends IterableFactory[LazySeq] {
  override def from[A](source: IterableOnce[A]): LazySeq[A] = {
    val builder: LazySeqBuilder[A] = new LazySeqBuilder[A]
    source.iterator.foreach { builder += _ }
    builder.result()
  }
}

trait LazySeqBase[+A] extends Iterable[A] with IterableOps[A, LazySeq, LazySeq[A]] { self: LazySeq[A] =>
  final override def iterableFactory: IterableFactory[LazySeq] = LazySeq

  final override def collect[B](pf: PartialFunction[A, B]): LazySeq[B] = new CollectedLazySeq(self, pf)
  final override def dropRight(n: Int): LazySeq[A] = if (n === 0) self else new DropRightLazySeq(self, n)
  final override def zipWithIndex: LazySeq[(A, Int)] = new ZipWithIndexLazySeq(self)
  final override def withFilter(p: A => Boolean): WithFilter[A, LazySeq] = new FilteredLazySeq(self, p)
  //final override def filter(p: A => Boolean): LazySeq[A] = withFilter(p) flatMap  (self)
  //final override def filterNot(p: A => Boolean): LazySeq[A] = withFilter { !p(_) } flatMap  (self)

  override def fromSpecific(it: IterableOnce[A @uncheckedVariance]): LazySeq[A] =
    iterableFactory.from[A](it)

  override protected[this] def newSpecificBuilder: Builder[A, LazySeq[A]] =
    LazySeq.newBuilder[A]

  override def empty: LazySeq[A] = EmptyLazySeq
}
