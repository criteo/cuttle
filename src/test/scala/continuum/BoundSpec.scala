package continuum

import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{PropSpec, Matchers}

import continuum.test.Generators

class BoundSpec
  extends PropSpec
  with GeneratorDrivenPropertyChecks
  with Matchers
  with Generators {

  property("The above relation on bounds is transitive.") {
    forAll { (a: Bound[Int], b: Bound[Int], c: Bound[Int]) =>
      val (upper, lower) = if (a isAbove b) (a, b) else (b, a)
      whenever(lower isAbove c) { (upper isAbove c) should be (true) }
    }
  }

  property("The below relation on bounds is transitive.") {
    forAll { (a: Bound[Int], b: Bound[Int], c: Bound[Int]) =>
      val (lower, upper) = if (a isBelow b) (a, b) else (b, a)
      whenever(upper isBelow c) { (lower isBelow c) should be (true) }
    }
  }
}
