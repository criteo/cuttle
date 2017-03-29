package continuum

import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{Matchers, PropSpec}

import continuum.test.Generators

class IntervalSetSpec
  extends PropSpec
  with GeneratorDrivenPropertyChecks
  with Matchers
  with Generators {

  property("An interval set should contain all of its constituent intervals") {
    forAll { (intervals: List[Interval[Int]]) =>
      val intervalSet = IntervalSet(intervals:_*)
      intervals.forall(intervalSet) should be (true)
    }
  }

  property("An interval set coalesces its constituent intervals") {
    forAll { (intervals: List[Interval[Int]]) =>
      val intervalSet = IntervalSet(intervals:_*)
      intervals.size should be >= (intervalSet.size)
      for {
        a <- intervalSet
        b <- intervalSet if a != b
      } a unions b should be (false)
    }
  }

  property("An interval set does not contain an interval in its difference") {
    forAll { (set: IntervalSet[Int], interval: Interval[Int]) =>
      (set - interval) contains interval should be (false)
    }
  }

  property("An interval set intersected with an interval should contain only intervals in common.") {
    forAll { (set: IntervalSet[Int], interval: Interval[Int]) =>
      val intersection = set intersect interval
      forAll { i: Interval[Int] =>
        if (set(i) && interval.encloses(i)) intersection(i) should be (true)
        else intersection(i) should be (false)
      }
    }
  }

  property("An interval set intersected with an interval set should contain only intervals in common.") {
    forAll { (a: IntervalSet[Int], b: IntervalSet[Int]) =>
      val intersection = a intersect b
      forAll { i: Interval[Int] =>
        if (a(i) && b(i)) intersection(i) should be (true)
        else intersection(i) should be (false)
      }
    }
  }

  property("The span of an interval set  all intervals in the interval set.") {
    forAll { (set: IntervalSet[Int]) =>
      val span = set.span
      forAll { i: Interval[Int] =>
        if (set(i)) span.get.encloses(i) should be (true)
      }
    }
  }
}
