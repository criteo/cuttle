package com.criteo.cuttle.timeseries.intervals

import scala.math.Ordering.Implicits._

import cats.implicits._

import io.circe._
import io.circe.syntax._
import io.circe.generic.semiauto._

import Bound.{Bottom, Finite, Top}

import scala.util._

private[timeseries] case class Interval[V: Ordering](lo: Bound[V], hi: Bound[V]) {
  if (lo >= hi)
    throw new IllegalArgumentException("low bound must be smaller than high bound")

  def intersects(other: Interval[V]) =
    !((this.lo >= other.hi) || (this.hi <= other.lo))
  def map[A: Ordering](f: V => A): Interval[A] =
    Interval(lo.map(f), hi.map(f))
  def toPair: (V, V) = (lo, hi) match {
    case (Finite(lo), Finite(hi)) => (lo, hi)
    case _                        => throw new IllegalArgumentException("cant convert infinite intervals to pair")
  }
}
private[timeseries] object Interval {
  def apply[V: Ordering](lo: V, hi: V): Interval[V] =
    Interval(Finite(lo), Finite(hi))
  def full[V: Ordering] = Interval[V](Bottom, Top)

  implicit def encoder[V: Ordering: Encoder]: Encoder[Interval[V]] =
    new Encoder[Interval[V]] {
      def apply(interval: Interval[V]) = Json.arr(
        interval.lo.asJson,
        interval.hi.asJson
      )
    }

  implicit def decoder[V: Ordering: Decoder]: Decoder[Interval[V]] =
    Decoder.decodeArray[Bound[V]].emapTry { array =>
      Try {
        val Array(lo, hi) = array
        Interval(lo, hi)
      }
    }
    // For backward compatibility we fallback to the generated decoder
    .or(deriveDecoder[Interval[V]])
}
