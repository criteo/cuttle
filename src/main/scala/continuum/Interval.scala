package continuum

import scala.language.implicitConversions

import continuum.bound.{Closed, Open, Unbounded}

/**
 * A non-empty bounded interval over a continuous, infinite, total-ordered set of values. An
 * interval contains all values between its lower and upper bound. The lower and/or upper bound may
 * be unbounded. Any operation which could potentially return an empty interval returns an Option
 * type instead.
 *
 * @param lower bounding ray of interval. Must point in the `Greater` direction.
 * @param upper bounding ray of interval. Must point in the `Lesser` direction.
 * @tparam T type of values contained in the continuous, infinite, total-ordered set which the
 *           interval operates on.
 */
final case class Interval[T](lower: GreaterRay[T], upper: LesserRay[T])(implicit conv: T=>Ordered[T])
  extends (T => Boolean)
  with Ordered[Interval[T]] {

  require(Interval.validate(lower, upper), "Invalid interval rays: " + lower + ", " + upper + ".")

  /**
   * Tests if this interval contains the specified point.
   */
  override def apply(point: T): Boolean = lower(point) && upper(point)

  /**
   * Tests if this interval intersects the other. Intervals intersect if they share any points in
   * common. Said another way, intervals intersect if they overlap.
   *
   * a0 <= b1 && b0 <= a1
   */
  def intersects(other: Interval[T]): Boolean =
    (lower intersects other.upper) && (upper intersects other.lower)

  /**
   * Returns the intersection of this interval and the other, or `None` if the intersection does not
   * exist.
   */
  def intersect(other: Interval[T]): Option[Interval[T]] =
    if (intersects(other)) {
      val l = if (lower encloses other.lower) other.lower else lower
      val u = if (upper encloses other.upper) other.upper else upper
      if ((l == lower) && (u == upper)) Some(this)
      else if ((l == other.lower) && (u == other.upper)) Some(other)
      else Some(Interval(l, u))
    } else None

  /**
   * Tests if this interval is tangent to the other. Intervals are tangent if they do not contain
   * any points in common, but the span of the intervals does not contain any points not in one of
   * the intersections.
   */
  def tangents(other: Interval[T]): Boolean =
    (lower tangents other.upper) || (upper tangents other.lower)

  /**
   * Tests if this interval unions the other. Intervals union if all the points contained by their
   * span are contained by one of the intervals. Said another way, intervals union if they
   * overlap or are tangent.
   */
  def unions(other: Interval[T]): Boolean = intersects(other) || tangents(other)

  /**
   * Returns the union of this interval and the other. If the intervals do not union, the empty
   * interval is returned. The union of an interval with the empty interval is the empty interval.
   */
  def union(other: Interval[T]): Option[Interval[T]] =
    if (unions(other)) Some(span(other)) else None

  /**
   * Returns the minimum spanning interval of this interval and the other interval. An interval
   * spans a pair of intervals if it encloses both. The span of an interval with the empty interval
   * is the empty interval.
   */
  def span(other: Interval[T]): Interval[T] = {
    val l = if (lower encloses other.lower) lower else other.lower
    val u = if (upper encloses other.upper) upper else other.upper
    if ((l == lower) && (u == upper)) this
    else if ((l == other.lower) && (u == other.upper)) other
    else Interval(l, u)
  }

  /**
   * Tests if this interval encloses the other. An interval encloses another if it contains all
   * points contained by the other. The union of an interval with an enclosed interval is the
   * enclosing interval. The intersection of an interval with an enclosed interval is the enclosed
   * interval. No interval encloses the empty interval.
   */
  def encloses(other: Interval[T]): Boolean =
    (lower encloses other.lower) && (upper encloses other.upper)

  /**
   * Intervals are compared first by their upper rays, and then by their lower rays.
   */
  def compare(other: Interval[T]): Int = {
    val c = lower compare other.lower
    if (c != 0) c
    else upper compare other.upper
  }

  /**
   * Returns the difference between this interval and the other. The set may contain 0, 1, or 2
   * resulting intervals.
   */
  def difference(other: Interval[T]): Set[Interval[T]] =
    intersect(other).fold(Set(this)) { intersection =>
      val left = intersection.lesser.flatMap(intersect)
      val right = intersection.greater.flatMap(intersect)
      Set(left, right).flatten
    }

  /**
   * Returns an interval which encompasses all values less than this interval, if such an interval
   * exists.
   */
  def lesser: Option[Interval[T]] =
    lower.tangent.map(upper => Interval(GreaterRay(Unbounded()), upper))

  /**
   * Returns an interval which encompasses all values greater than this interval, if such an
   * interval exists.
   */
  def greater: Option[Interval[T]] =
    upper.tangent.map(lower => Interval(lower, LesserRay(Unbounded())))

  /**
   * Returns a normalized form of this Interval, if possible. The lower bound of a normalized
   * interval is Closed and the upper bound of a normalized interval is Open. If this interval is
   * unbounded in some direction, then the corresponding normalized bound will be None.
   */
  def normalize(implicit discrete: Discrete[T]): (Option[T], Option[T]) = {
    val l = lower.bound match {
      case Closed(cut) => Some(cut)
      case Open(cut)   => discrete.next(cut)
      case _ => None
    }
    val u = upper.bound match {
      case Closed(cut) => discrete.next(cut)
      case Open(cut) => Some(cut)
      case _ => None
    }
    l -> u
  }

  /**
   * Tests if this interval encloses only a single discrete point.
   */
  def isPoint: Boolean = (lower.bound, upper.bound) match {
    case (Closed(l), Closed(u)) if l == u => true
    case _ => false
  }

  /**
   * Returns the discrete value enclosed by this interval, if it is a point.
   */
  def point: Option[T] = (lower.bound, upper.bound) match {
    case (Closed(l), Closed(u)) if l == u => Some(l)
    case _ => None
  }

  /**
   * Transform the bounds of this interval to create a new Interval. The resulting interval must be
   * valid, i.e., the transformation must keep the relative order of the bounds.
   */
  def map[U](f: T => U)(implicit conv: U => Ordered[U]): Interval[U] =
    Interval(GreaterRay(lower.bound.map(f)), LesserRay(upper.bound.map(f)))

  /**
   * Converts this interval to a [[scala.collection.immutable.Range]], if possible.
   *
   * @throws IllegalArgumentException if the resulting range would contain more than
   *                                  [[scala.Int.MaxValue]] elements.
   */
  def toRange(implicit num: Numeric[T]): Range = {
    val start: Int = lower.bound match {
      case Closed(c) => num.toInt(c)
      case Open(c)   => {
        val i = num.toInt(c)
        if (i == Int.MaxValue) return Range(Int.MaxValue, Int.MaxValue)
        else i + 1
      }
      case Unbounded() => Int.MinValue
    }
    upper.bound match {
      case Closed(c) => Range.inclusive(start, num.toInt(c))
      case Open(c)   => Range(start, num.toInt(c))
      case Unbounded() => Range.inclusive(start, Int.MaxValue)
    }
  }

  override def toString(): String = {
    def lowerString: String = lower.bound match {
      case Closed(cut) => "[" + cut.toString
      case Open(cut) => "(" + cut.toString
      case Unbounded() => "(-∞"

    }
    def upperString: String = upper.bound match {
      case Closed(cut) => cut.toString + "]"
      case Open(cut) => cut.toString + ")"
      case Unbounded() => "∞)"
    }
    (lower.bound, upper.bound) match {
      case (Closed(l), Closed(u)) if l == u => "[" + l + "]"
      case _ => lowerString + ", " + upperString
    }
  }
}

object Interval {

  private[continuum] def validate[T](lower: Ray[T], upper: Ray[T])(implicit conv: T=>Ordered[T]): Boolean =
    lower intersects upper

  def open[T](lower: T, upper: T)(implicit conv: T=>Ordered[T]): Interval[T] =
    Interval(GreaterRay(Open(lower)), LesserRay(Open(upper)))

  def closed[T](lower: T, upper: T)(implicit conv: T=>Ordered[T]): Interval[T] =
    Interval(GreaterRay(Closed(lower)), LesserRay(Closed(upper)))

  def openClosed[T](lower: T, upper: T)(implicit conv: T=>Ordered[T]): Interval[T] =
    Interval(GreaterRay(Open(lower)), LesserRay(Closed(upper)))

  def closedOpen[T](lower: T, upper: T)(implicit conv: T=>Ordered[T]): Interval[T] =
    Interval(GreaterRay(Closed(lower)), LesserRay(Open(upper)))

  def greaterThan[T](cut: T)(implicit conv: T=>Ordered[T]): Interval[T] =
    Interval(GreaterRay(Open(cut)), LesserRay(Unbounded()))

  def atLeast[T](cut: T)(implicit conv: T=>Ordered[T]): Interval[T] =
    Interval(GreaterRay(Closed(cut)), LesserRay(Unbounded()))

  def lessThan[T](cut: T)(implicit conv: T=>Ordered[T]): Interval[T] =
    Interval(GreaterRay(Unbounded()), LesserRay(Open(cut)))

  def atMost[T](cut: T)(implicit conv: T=>Ordered[T]): Interval[T] =
    Interval(GreaterRay(Unbounded()), LesserRay(Closed(cut)))

  def full[T](implicit conv: T=>Ordered[T]): Interval[T] =
    Interval(GreaterRay(Unbounded()), LesserRay(Unbounded()))

  def all[T](implicit conv: T=>Ordered[T]): Interval[T] = full

  def point[T](point: T)(implicit conv: T=>Ordered[T]): Interval[T] = closed(point, point)

  def apply[T](implicit conv: T=>Ordered[T]): Interval[T] = full

  def apply[T](point: T)(implicit conv: T=>Ordered[T]): Interval[T] = closed(point, point)

  implicit def fromTuple[T](tuple: (T, T))(implicit conv: T=>Ordered[T]): Interval[T] =
    closedOpen(tuple._1, tuple._2)

  implicit def fromRange(range: Range): Interval[Int] = {
    require(range.step == 1, "Range must be continuous.")
    if(range.isInclusive) closed(range.start, range.end)
    else closedOpen(range.start, range.end)
  }

  def rightOrdering[T]: Ordering[Interval[T]] = new Ordering[Interval[T]] {
    def compare(a: Interval[T], b: Interval[T]): Int = {
      val c = a.upper compare b.upper
      if (c != 0) c
      else a.lower compare b.lower
    }
  }
}
