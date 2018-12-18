package com.criteo.cuttle.timeseries.intervals

import scala.math.Ordered.orderingToOrdered

import cats._
import de.sciss.fingertree.{FingerTree, Measure}
import FingerTree._
import Bound.{Bottom, Top}
import cats.FunctorFilter
import cats.implicits._

private[timeseries] object IntervalMap {

  private class MeasureKey[A, B] extends Measure[(A, B), Option[A]] {
    override def toString = "Key"
    val zero: Option[A] = None
    def apply(x: (A, B)) = Some(x._1)

    def |+|(a: Option[A], b: Option[A]) = b
  }
  private implicit def measure[A, B] = new MeasureKey[Interval[A], B]

  private type Elem[A, B] = (Interval[A], B)

  def apply[A: Ordering, B](elems: Elem[A, B]*): IntervalMap[A, B] = {
    val sortedElems = elems.sortBy(_._1.lo)
    val isOverlapping = sortedElems.sliding(2).exists {
      case List((Interval(_, hi), v1), (Interval(lo, _), v2)) =>
        hi > lo || (hi == lo && v1 == v2)
      case _ => false
    }
    if (isOverlapping)
      throw new IllegalArgumentException("non-canonical intervals")

    new Impl(FingerTree(sortedElems: _*))
  }

  def empty[A: Ordering, B]: IntervalMap[A, B] =
    new Impl(FingerTree.empty)

  private class Impl[A: Ordering, B](val tree: FingerTree[Option[Interval[A]], Elem[A, B]]) extends IntervalMap[A, B] {
    def toList = tree.toList

    def lower(interval: Interval[A]): (Option[Interval[A]] => Boolean) = {
      case None                  => true
      case Some(Interval(lo, _)) => lo < interval.lo
    }

    def greater(interval: Interval[A]): Option[Interval[A]] => Boolean = {
      case None                  => true
      case Some(Interval(_, hi)) => hi <= interval.hi
    }

    def intersect(interval: Interval[A]) = {
      val (leftOfLow, rightOfLow) = tree.span(lower(interval))

      val withoutLow = leftOfLow.viewRight match {
        case ViewRightCons(_, (Interval(_, hi), v)) if hi > interval.lo =>
          (Interval(interval.lo, hi) -> v) +: rightOfLow
        case _ =>
          rightOfLow
      }
      val (leftOfHigh, rightOfHigh) = withoutLow.span(greater(interval))

      val newTree = rightOfHigh.viewLeft match {
        case ViewLeftCons((Interval(lo, _), v), _) if lo < interval.hi =>
          leftOfHigh :+ (Interval(lo, interval.hi) -> v)
        case _ =>
          leftOfHigh
      }

      new Impl(newTree)
    }

    def update(interval: Interval[A], value: B) = {
      val left = tree.takeWhile(lower(interval))
      val (remainingLeft, newLo) = left.viewRight match {
        case ViewRightCons(init, (Interval(lo, hi), v)) if hi >= interval.lo =>
          if (v == value)
            (init, lo)
          else
            (init :+ (Interval(lo, interval.lo) -> v), interval.lo)
        case _ =>
          (left, interval.lo)
      }

      val right = tree.dropWhile(greater(interval))
      val (newHi, remainingRight) = right.viewLeft match {
        case ViewLeftCons((Interval(lo, hi), v), tail) if lo <= interval.hi =>
          if (v == value)
            (hi, tail)
          else
            (interval.hi, (Interval(interval.hi, hi) -> v) +: tail)
        case _ =>
          (interval.hi, right)
      }

      new Impl((remainingLeft :+ (Interval(newLo, newHi) -> value)) ++ remainingRight)
    }

    def mapKeys[K: Ordering](f: A => K) =
      new Impl(FingerTree(tree.toList.map {
        case (interval, v) => (interval.map(f) -> v)
      }: _*))

    def whenIsDef[C](other: IntervalMap[A, C]) =
      new Impl(FingerTree((for {
        (interval, _) <- other.toList
        m <- this.intersect(interval).toList
      } yield m): _*))

    def whenIsUndef[C](other: IntervalMap[A, C]) = {
      val bounds = for {
        (Interval(lo, hi), _) <- other.toList
        b <- List(lo, hi)
      } yield b
      val newBounds =
        if (bounds.length == 0) {
          List(Bottom, Top)
        } else {
          val first = bounds.head
          val last = bounds.last
          val middle = bounds.tail.take(bounds.length - 2)
          (if (first == Bottom) List() else List(Bottom, first)) ++
            middle ++
            (if (last == Top) List() else List(last, Top))
        }
      val newIntervals = newBounds.grouped(2).toList.collect { case List(l, h) if l != h => Interval(l, h) }
      new Impl(FingerTree((for {
        interval <- newIntervals
        m <- this.intersect(interval).toList
      } yield m): _*))
    }
  }

  implicit def functorInstance[K: Ordering] = new Functor[({ type Alias[a] = IntervalMap[K, a] })#Alias] {
    override def map[A, B](m: IntervalMap[K, A])(f: A => B): IntervalMap[K, B] = m match {
      case impl: Impl[K, A] =>
        new Impl(FingerTree[Option[Interval[K]], Elem[K, B]](impl.tree.toList.map {
          case (itvl, v) => itvl -> f(v)
        }: _*))
    }
  }

  implicit def functorEmptyInstance[K: Ordering] = new FunctorFilter[({ type Alias[a] = IntervalMap[K, a] })#Alias] {
    override val functor = implicitly[Functor[({ type Alias[a] = IntervalMap[K, a] })#Alias]]

    override def mapFilter[A, B](fa: IntervalMap[K, A])(f: A => Option[B]) = fa match {
      case impl: Impl[K, A] =>
        new Impl(FingerTree[Option[Interval[K]], Elem[K, B]](impl.tree.toList.mapFilter {
          case (itvl, v) => f(v).map(itvl -> _)
        }: _*))
    }

    override def collect[A, B](fa: IntervalMap[K, A])(f: PartialFunction[A, B]) = mapFilter(fa)(f.lift)

    override def flattenOption[A](fa: IntervalMap[K, Option[A]]) = mapFilter(fa)(identity)

    override def filter[A](fa: IntervalMap[K, A])(f: A => Boolean) = mapFilter(fa)(a => Some(a).filter(f))
  }

  implicit def eqInstance[K: Ordering, A] = Eq.fromUniversalEquals[IntervalMap[K, A]]
}

private[timeseries] sealed trait IntervalMap[A, B] {
  import IntervalMap._

  def toList: List[Elem[A, B]]
  def update(interval: Interval[A], value: B): IntervalMap[A, B]
  def intersect(interval: Interval[A]): IntervalMap[A, B]
  def mapKeys[K: Ordering](f: A => K): IntervalMap[K, B]
  def whenIsDef[C](other: IntervalMap[A, C]): IntervalMap[A, B]
  def whenIsUndef[C](other: IntervalMap[A, C]): IntervalMap[A, B]
}
