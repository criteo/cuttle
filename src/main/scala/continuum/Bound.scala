package continuum

/**
 * A `Bound` is a lower or upper bound over a continuous and infinite set of total-ordered values.
 * A bound can be closed, open, or unbounded.  An unbounded bound represents a bound either above or
 * below all other bounds, depending on whether the bound is an upper or lower bound.
 *
 * `Bound` is an internal implementation mechanism for `Ray`.
 *
 * @tparam T type of values contained in the continuous, infinite, total-ordered set which the
 *           bound operates on.
 */
sealed abstract class Bound[T](implicit conv: T=>Ordered[T]) {
  /**
   * Returns `true` if all points below the other bound are also below this bound.
   */
  def isAbove(other: Bound[T]): Boolean

  /**
   * Returns `true` if all points above the other bound are also above this bound.
   */
  def isBelow(other: Bound[T]): Boolean

  /**
   * Returns `true` if this is an unbounded bound.
   */
  def isUnbounded: Boolean

  /**
   * Returns `true` if this is a bound is not unbounded.
   */
  def isBounded: Boolean = !isUnbounded

  /**
   * Tranform this bound.
   */
  def map[U](f: T => U)(implicit conv: U=>Ordered[U]): Bound[U]
}

package bound {

  case class Closed[T](value: T)(implicit conv: T=>Ordered[T]) extends Bound[T] {
    override def isAbove(otherBound: Bound[T]): Boolean = otherBound match {
      case Open(otherC) => value >= otherC
      case Closed(otherC) => value >= otherC
      case Unbounded() => false
    }

    override def isBelow(otherBound: Bound[T]): Boolean = otherBound match {
      case Open(otherC) => value <= otherC
      case Closed(otherC) => value <= otherC
      case Unbounded() => false
    }

    override def isUnbounded: Boolean = false

    def map[U](f: (T) => U)(implicit conv: U=>Ordered[U]): Bound[U] = Closed(f(value))
  }

  case class Open[T](value: T)(implicit conv: T=>Ordered[T]) extends Bound[T] {
    override def isAbove(otherBound: Bound[T]): Boolean = otherBound match {
      case Open(otherC) => value >= otherC
      case Closed(otherC) => value > otherC
      case Unbounded() => false
    }

    override def isBelow(otherBound: Bound[T]): Boolean = otherBound match {
      case Open(otherC) => value <= otherC
      case Closed(otherC) => value < otherC
      case Unbounded() => false
    }

    override def isUnbounded: Boolean = false

    def map[U](f: (T) => U)(implicit conv: U=>Ordered[U]): Bound[U] = Open(f(value))
  }

  case class Unbounded[T]()(implicit conv: T=>Ordered[T]) extends Bound[T] {
    override def isBelow(other: Bound[T]): Boolean = true
    override def isAbove(other: Bound[T]): Boolean = true
    override def isUnbounded: Boolean = true
    def map[U](f: (T) => U)(implicit conv: U=>Ordered[U]): Bound[U] = Unbounded[U]()
  }
}
