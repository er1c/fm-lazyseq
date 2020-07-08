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
import scala.collection.{GenTraversableOnce, Traversable, TraversableLike}
import scala.collection.generic._
import scala.collection.mutable.Builder

trait LazySeqObjBase[CC[X] <: Traversable[X] with GenericTraversableTemplate[X, CC]] extends TraversableFactory[CC]

trait LazySeqBase[+A]
  extends Traversable[A] with GenericTraversableTemplate[A, LazySeq] with TraversableLike[A, LazySeq[A]] {
  self: LazySeq[A] =>
  final override def companion: GenericCompanion[LazySeq] = LazySeq
  final override def seq: LazySeq[A] = self
  final def collect[B](pf: PartialFunction[A, B]): LazySeq[B] = new CollectedLazySeq(self, pf)
  final def dropRight(n: Int): LazySeq[A] = if (n === 0) this else new DropRightLazySeq(self, n)
  final def zipWithIndex: LazySeq[(A, Int)] = new ZipWithIndexLazySeq(self)
  final override def withFilter(p: A => Boolean): LazySeq[A] = new FilteredLazySeq(self, p)
  final override def filter(p: A => Boolean): LazySeq[A] = withFilter(p)
  final override def filterNot(p: A => Boolean): LazySeq[A] = withFilter { !p(_) }

  final override protected[this] def newBuilder(): Builder[A, LazySeq[A]] = new LazySeqBuilder[A]()

  final override def map[B, That](f: A => B)(implicit bf: CanBuildFrom[LazySeq[A], B, That]): That =
    new MappedLazySeq(this, f).asInstanceOf[That]

  final override def flatMap[B, That](f: A => GenTraversableOnce[B])(implicit
    bf: CanBuildFrom[LazySeq[A], B, That]): That =
    new FlatMappedLazySeq(this, f).asInstanceOf[That]
}
