package continuum

import scala.reflect.ClassTag

/**
 * A trait for describing discrete domains.
 */
trait Discrete[T] {
  def next(value: T): Option[T]
}

object Discrete {
  /**
   * An implementation of the Discrete trait for longs.
   */
  implicit object DiscreteLong extends Discrete[Long] {
    override def next(long: Long): Option[Long] = if (long == Long.MaxValue) None else Some(long + 1)
  }

  /**
   * An implementation of the Discrete trait for ints.
   */
  implicit object DiscreteInt extends Discrete[Int] {
    override def next(int: Int): Option[Int] = if (int == Int.MaxValue) None else Some(int + 1)
  }

  /**
   * An implementation of the Discrete trait for arrays.
   */
  implicit def DiscreteArray[T : ClassTag]: Discrete[Array[T]] = new Discrete[Array[T]] {
    override def next(value: Array[T]): Option[Array[T]] = {
      val ary = new Array[T](value.length + 1)
      value.copyToArray(ary)
      Some(ary)
    }
  }
}
