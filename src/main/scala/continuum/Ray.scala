package continuum

import continuum.bound.{Closed, Open, Unbounded}

/**
 * A bounded subset of a continuous, infinite, and total-ordered values. A ray is composed of a
 * single bound and a direction. The ray may either point in the `Lesser` direction, towards smaller
 * values, or in the `Greater` direction, towards larger values. Thus, if the ray points in the
 * `Greater` direction, it is bounded below, whereas a ray pointing in the `Greater` direction is
 * bounded above. A ray's bound can potentially be unbounded, in which case the ray is equivalent to
 * a line.
 *
 * @tparam T type of values contained in the continuous, infinite, total-ordered set which the
 *           ray operates on.
 */
sealed abstract class Ray[T](implicit conv: T => Ordered[T]) extends (T => Boolean) {

  def bound: Bound[T]

  /**
   * Tests if this ray contains the specified point.
   */
  override def apply(point: T): Boolean

  /**
   * Tests if this ray encloses the other. A ray encloses another if it contains all points
   * contained by the other.
   */
  def encloses(other: Ray[T]): Boolean

  /**
   * Tests if this ray intersects the other. Rays intersect if they share any points in common.
   * Said another way, rays intersect if they overlap.
   */
  def intersects(other: Ray[T]): Boolean

  /**
   * Tests if this ray is tangent to the other. Rays are tangent if they do not contain
   * any points in common, but all points are contained by one of the rays.
   */
  def tangents(other: Ray[T]): Boolean

  /**
   * Tests if this ray is connected to the other.  Rays are connected if they intersect, or are
   * tangent.
   */
  def connects(other: Ray[T]): Boolean = intersects(other) || tangents(other)

  /**
   * Returns the ray tangent to this one, if such a ray exists.
   */
  def tangent: Option[Ray[T]]

  def isSameDirection(other: Ray[T]): Boolean
}

case class GreaterRay[T](bound: Bound[T])(implicit conv: T => Ordered[T]) extends Ray[T] with Ordered[GreaterRay[T]] {

  override def apply(point: T): Boolean = bound match {
    case Closed(value) => point >= value
    case Open(value)   => point > value
    case Unbounded()   => true
  }

  override def encloses(other: Ray[T]): Boolean =
    if (bound.isUnbounded) true
    else if (isSameDirection(other)) bound isBelow other.bound
    else false

  override def intersects(other: Ray[T]): Boolean =
    if (bound.isUnbounded || other.bound.isUnbounded) true
    else if (isSameDirection(other)) true
    else (bound, other.bound) match {
      case (Closed(a), Closed(b)) => a <= b
      case (Closed(a), Open(b))   => a < b
      case (Open(a), Closed(b))   => a < b
      case (Open(a), Open(b))     => a < b
      case _ => throw new AssertionError()
    }

  override def tangents(other: Ray[T]): Boolean =
    if (bound.isUnbounded || other.bound.isUnbounded) false
    else if (isSameDirection(other)) false
    else (bound, other.bound) match {
      case (Closed(a), Open(b)) if a == b => true
      case (Open(a), Closed(b)) if a == b => true
      case _                              => false
    }

  override def tangent: Option[LesserRay[T]] = bound match {
    case Closed(p)   => Some(LesserRay(Open(p)))
    case Open(p)     => Some(LesserRay(Closed(p)))
    case Unbounded() => None
  }

  override def isSameDirection(other: Ray[T]): Boolean = other match {
    case GreaterRay(_) => true
    case _             => false
  }

  override def compare(other: GreaterRay[T]): Int = {
    if (this == other) 0
    else if (this encloses other) -1
    else 1
  }

  override def toString(): String = {
    val s = bound match {
      case Closed(p)   => "["+ p +"]"
      case Open(p)     => "("+ p +")"
      case Unbounded() => "(∞)"
    }
    s + "----->"
  }
}

case class LesserRay[T](bound: Bound[T])(implicit conv: T => Ordered[T]) extends Ray[T] with Ordered[LesserRay[T]] {

  override def apply(point: T): Boolean = bound match {
    case Closed(value) => point <= value
    case Open(value)   => point < value
    case Unbounded()   => true
  }

  override def encloses(other: Ray[T]): Boolean =
    if (bound.isUnbounded) true
    else if (isSameDirection(other)) bound isAbove other.bound
    else false

  override def intersects(other: Ray[T]): Boolean =
    if (bound.isUnbounded || other.bound.isUnbounded) true
    else if (isSameDirection(other)) true
    else (bound, other.bound) match {
      case (Closed(a), Closed(b)) => a >= b
      case (Closed(a), Open(b))   => a > b
      case (Open(a), Closed(b))   => a > b
      case (Open(a), Open(b))     => a > b
      case _ => throw new AssertionError()
    }

  override def tangents(other: Ray[T]): Boolean =
    if (bound.isUnbounded || other.bound.isUnbounded) false
    else if (isSameDirection(other)) false
    else (bound, other.bound) match {
      case (Closed(a), Open(b)) if a == b => true
      case (Open(a), Closed(b)) if a == b => true
      case _ => false
    }

  override def tangent: Option[GreaterRay[T]] = bound match {
    case Closed(p)   => Some(GreaterRay(Open(p)))
    case Open(p)     => Some(GreaterRay(Closed(p)))
    case Unbounded() => None
  }

  override def isSameDirection(other: Ray[T]): Boolean = other match {
    case LesserRay(_) => true
    case _            => false
  }

  override def compare(other: LesserRay[T]): Int = {
    if (this == other) 0
    else if (this encloses other) 1
    else -1
  }

  override def toString(): String = {
    val s = bound match {
      case Closed(p)   => "["+ p +"]"
      case Open(p)     => "("+ p +")"
      case Unbounded() => "(∞)"
    }
    "<-----" + s
  }
}
