package continuum

import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{Matchers, PropSpec}

import continuum.bound.Unbounded
import continuum.test.Generators

class RaySpec
  extends PropSpec
  with GeneratorDrivenPropertyChecks
  with Matchers
  with Generators {

  property("a greater ray encloses a greater ray which it starts before.") {
    forAll { (a: Bound[Int], b: Bound[Int]) =>
      whenever (a isBelow b) {
        GreaterRay(a) encloses GreaterRay(b) should be (true)
      }
    }
  }

  property("a lesser ray encloses a lesser ray which it starts after.") {
    forAll { (a: Bound[Int], b: Bound[Int]) =>
      whenever (a isAbove b) {
        LesserRay(a) encloses LesserRay(b) should be (true)
      }
    }
  }


  property("a bounded ray does not enclose another ray pointing a different direction") {
    forAll { (a: Ray[Int], b: Ray[Int]) =>
      whenever (a.bound != Unbounded[Int]() && !a.isSameDirection(b)) {
        a encloses b should be (false)
      }
    }
  }

  property("an unbounded ray encloses all rays") {
    forAll { (bound: Unbounded[Int], other: Ray[Int]) =>
      GreaterRay(bound) encloses other should be (true)
      LesserRay(bound) encloses other should be (true)
    }
  }

  property("one of two rays in the same direction encloses the other") {
    forAll { (a: Ray[Int], b: Ray[Int]) =>
      whenever (a isSameDirection b) {
        (a encloses b) || (b encloses a) should be (true)
      }
    }
  }

  property("ray encloses is transitive") {
    forAll { (a: Ray[Int], b: Ray[Int], c: Ray[Int]) =>
      if (a encloses b) {
        if (b encloses c) a encloses c should be (true)
        if (c encloses a) c encloses b should be (true)
      }
      if (b encloses a) {
        if (a encloses c) b encloses c should be (true)
        if (c encloses b) c encloses a should be (true)
      }
      if (a encloses c) {
        if (c encloses b) a encloses b should be (true)
        if (b encloses a) b encloses c should be (true)
      }
      if (c encloses a) {
        if (a encloses b) c encloses b should be (true)
        if (b encloses c) b encloses a should be (true)
      }
      if (b encloses c) {
        if (c encloses a) b encloses a should be (true)
        if (a encloses b) a encloses c should be (true)
      }
      if (c encloses b) {
        if (b encloses a) c encloses a should be (true)
        if (a encloses c) a encloses b should be (true)
      }
    }
  }

  property("a value contained in an enclosed ray is contained by the enclosing ray") {
    forAll { (a: Ray[Int], b: Ray[Int], value: Int) =>
      whenever(a encloses b) {
        if (b(value)) a(value) should be (true)
      }
    }
  }

  property("rays with the same direction intersect") {
    forAll { (a: Bound[Int], b: Bound[Int]) =>
      GreaterRay(a) intersects GreaterRay(b) should be (true)
      LesserRay(a) intersects LesserRay(b) should be (true)
    }
  }

  property("ray intersection is commutative") {
    forAll { (a: Ray[Int], b: Ray[Int]) =>
      val intersects = b intersects a
      if (a intersects b) intersects should be (true)
      else intersects should be (false)
    }
  }

  property("ray connectedness is commutative") {
    forAll { (a: Ray[Int], b: Ray[Int]) =>
      val connects = b connects a
      if (a connects b) connects should be (true)
      else connects should be (false)
    }
  }

  property("rays which intersect are connected") {
    forAll { (a: Ray[Int], b: Ray[Int]) =>
      val connects = a intersects b
      if (a intersects b) connects should be (true)
      else connects should be (false)
    }
  }

  property("ray tangentness is commutative") {
    forAll { (a: Ray[Int], b: Ray[Int]) =>
      val tangent = b tangents a
      if (a tangents b) tangent should be (true)
      else tangent should be (false)
    }
  }

  property("tangent rays are connected, but do not intersect") {
    forAll { (a: Ray[Int], b: Ray[Int]) =>
      if (a tangents b) {
        a connects b should be (true)
        a intersects b should be (false)
      } else if (a connects b) a intersects b should be (true)

    }
  }
}
