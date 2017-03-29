package continuum.test

import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.{Shrink, Gen, Arbitrary}

import continuum.bound.{Closed, Open, Unbounded}
import continuum.{IntervalSet, Interval, Ray, Bound, LesserRay, GreaterRay}

trait Generators {

  implicit def arbOpenBound[T : Arbitrary](implicit conv: T => Ordered[T]): Arbitrary[Open[T]] =
    Arbitrary { for (cut <- arbitrary[T]) yield Open(cut) }

  implicit def arbClosedBound[T : Arbitrary](implicit conv: T => Ordered[T]): Arbitrary[Closed[T]] =
    Arbitrary { for (cut <- arbitrary[T]) yield Closed(cut) }

  implicit def arbUnbounded[T](implicit conv: T => Ordered[T]): Arbitrary[Unbounded[T]] = Arbitrary(Unbounded[T]())

  implicit def arbBound[T : Arbitrary](implicit conv: T => Ordered[T]): Arbitrary[Bound[T]] =
    Arbitrary(Gen.frequency(
      4 -> arbitrary[Closed[T]],
      4 -> arbitrary[Open[T]],
      1 -> arbitrary[Unbounded[T]]))

  implicit def arbLesserRay[T : Arbitrary](implicit conv: T => Ordered[T]): Arbitrary[LesserRay[T]] =
    Arbitrary(for (b <- arbitrary[Bound[T]]) yield LesserRay(b))

  implicit def arbGreaterRay[T : Arbitrary](implicit conv: T => Ordered[T]): Arbitrary[GreaterRay[T]] =
    Arbitrary(for (b <- arbitrary[Bound[T]]) yield GreaterRay(b))

  implicit def arbRay[T : Arbitrary](implicit conv: T => Ordered[T]): Arbitrary[Ray[T]] =
      Arbitrary(Gen.oneOf(arbitrary[GreaterRay[T]], arbitrary[LesserRay[T]]))

  implicit def arbInterval[T: Arbitrary](implicit conv: T => Ordered[T]): Arbitrary[Interval[T]] = Arbitrary {
    def validate(a: Bound[T], b: Bound[T]) =
      Interval.validate(GreaterRay(a), LesserRay(b)) ||
      Interval.validate(GreaterRay(b), LesserRay(a))
    def interval(a: Bound[T], b: Bound[T]) =
      if (Interval.validate(GreaterRay(a), LesserRay(b))) new Interval(GreaterRay(a), LesserRay(b))
      else new Interval(GreaterRay(b), LesserRay(a))
    for {
      a <- arbitrary[Bound[T]]
      b <- arbitrary[Bound[T]] if validate(a, b)
    } yield interval(a, b)
  }

  /**
   * Generates arbitrary Interval[Int]s. This is functionally the same as the generic Interval
   * generator, but it is more efficient at generating valid intervals. It will automatically be
   * used by the ScalaCheck framework if both are in scope.
   */
  implicit def arbIntInterval: Arbitrary[Interval[Int]] = Arbitrary {
    def genOpenAbove(below: Bound[Int]): Gen[Bound[Int]] = below match {
      case Closed(l)   if l == Int.MaxValue => arbitrary[Unbounded[Int]]
      case Open(l)     if l == Int.MaxValue => arbitrary[Unbounded[Int]]
      case Closed(l)   => for(n <- Gen.choose(l + 1, Int.MaxValue)) yield Open(n)
      case Open(l)     => for(n <- Gen.choose(l + 1, Int.MaxValue)) yield Open(n)
      case Unbounded() => arbitrary[Open[Int]]
    }

    def genClosedAbove(below: Bound[Int]): Gen[Bound[Int]] = below match {
      case Open(l)     if l == Int.MaxValue => arbitrary[Unbounded[Int]]
      case Closed(l)   => for(n <- Gen.choose(l, Int.MaxValue)) yield Closed(n)
      case Open(l)     => for(n <- Gen.choose(l + 1, Int.MaxValue)) yield Closed(n)
      case Unbounded() => arbitrary[Closed[Int]]
    }

    def genLesserRay(g: GreaterRay[Int]): Gen[LesserRay[Int]] = Gen.frequency(
      4 -> genClosedAbove(g.bound),
      4 -> genOpenAbove(g.bound),
      1 -> arbitrary[Unbounded[Int]]
    ).map(LesserRay(_))

    for {
      lower <- arbitrary[GreaterRay[Int]]
      upper <- genLesserRay(lower)
    } yield new Interval(lower, upper)
  }

  implicit def shrinkGreaterRay[T : Shrink](implicit conv: T => Ordered[T]): Shrink[GreaterRay[T]] = Shrink { ray =>
    ray.bound match {
      case Closed(n)   => for (np <- Shrink.shrink(n)) yield GreaterRay(Closed(np))
      case Open(n)     => for (np <- Shrink.shrink(n)) yield GreaterRay(Open(np))
      case Unbounded() => Stream.empty
    }
  }

  implicit def shrinkLesserRay[T : Shrink](implicit conv: T => Ordered[T]): Shrink[LesserRay[T]] = Shrink { ray =>
    ray.bound match {
      case Closed(n)   => for (np <- Shrink.shrink(n)) yield LesserRay(Closed(np))
      case Open(n)     => for (np <- Shrink.shrink(n)) yield LesserRay(Open(np))
      case Unbounded() => Stream.empty
    }
  }

  implicit def shrinkInterval[T : Shrink](implicit conv: T => Ordered[T]): Shrink[Interval[T]] = Shrink { interval =>
    for {
      lower <- Shrink.shrink(interval.lower)
      upper <- Shrink.shrink(interval.upper) if Interval.validate(lower, upper)
    } yield new Interval(lower, upper)
  }

  implicit def arbRange: Arbitrary[Range] = Arbitrary {
    Gen.sized { size =>
      for {
        lower <- arbitrary[Int] if lower + size >= lower
      } yield Range.inclusive(lower, lower + size)
    }
  }

  /**
   * Generates ranges which can be converted to a [[scala.collection.immutable.Range]] with the
   * .toRange method. As opposed to the arbitrary Int interval generator, this ensures the created
   * ranges are of a reasonable size.
   */
  def genIntervalRange: Gen[Interval[Int]] = {
    def genOpen: Gen[Interval[Int]] = Gen.sized { size =>
      for {
        lower <- arbitrary[Int] if lower + size > lower
      } yield Interval.open(lower, lower + size)
    }

    def genClosed: Gen[Interval[Int]] = Gen.sized { size =>
      for {
        lower <- arbitrary[Int] if lower + size >= lower
      } yield Interval.closed(lower, lower + size)
    }

    def genOpenClosed: Gen[Interval[Int]] = Gen.sized { size =>
      for {
        lower <- arbitrary[Int] if lower + size > lower
      } yield Interval.openClosed(lower, lower + size)
    }

    def genClosedOpen: Gen[Interval[Int]] = Gen.sized { size =>
      for {
        lower <- arbitrary[Int] if lower + size > lower
      } yield Interval.closedOpen(lower, lower + size)
    }

    def closedUnbounded: Gen[Interval[Int]] =
      Gen.sized(size => Interval.atLeast(Int.MaxValue - size))

    def openUnbounded: Gen[Interval[Int]] =
      Gen.sized(size => Interval.greaterThan(Int.MaxValue - size))

    def unboundedClosed: Gen[Interval[Int]] =
      Gen.sized(size => Interval.atMost(Int.MinValue + size))

    def unboundedOpen: Gen[Interval[Int]] =
      Gen.sized(size => Interval.lessThan(Int.MinValue + size))

    Gen.frequency(
      4 -> genOpen,
      4 -> genClosed,
      4 -> genOpenClosed,
      4 -> genClosedOpen,
      1 -> closedUnbounded,
      1 -> openUnbounded,
      1 -> unboundedClosed,
      1 -> unboundedOpen)
  }

  implicit def arbIntervalSet[T : Arbitrary](implicit conv: T => Ordered[T]): Arbitrary[IntervalSet[T]] =
    Arbitrary(for (intervals <- arbitrary[Array[Interval[T]]]) yield IntervalSet(intervals:_*))

  /**
   * Generates arbitrary interval sets of Ints using the more efficient Int interval generator.
   */
  implicit def arbIntIntervalSet: Arbitrary[IntervalSet[Int]] =
    Arbitrary(for (intervals <- arbitrary[Array[Interval[Int]]]) yield IntervalSet(intervals:_*))
}
