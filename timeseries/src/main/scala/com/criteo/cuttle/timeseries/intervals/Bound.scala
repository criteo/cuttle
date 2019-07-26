package com.criteo.cuttle.timeseries.intervals

import cats._

import io.circe._
import io.circe.syntax._
import io.circe.generic.semiauto._

import scala.util._

private[timeseries] sealed trait Bound[+V]
private[timeseries] object Bound {
  case object Bottom extends Bound[Nothing]
  case object Top extends Bound[Nothing]
  case class Finite[V](bound: V) extends Bound[V]
  implicit def ordering[V: Ordering]: Ordering[Bound[V]] =
    Ordering.by[Bound[V], (Int, Option[V])] {
      case Bottom    => (-1, None)
      case Top       => (1, None)
      case Finite(v) => (0, Some(v))
    }
  implicit def functorInstance: Functor[Bound] = new Functor[Bound] {
    def map[A, B](bound: Bound[A])(f: A => B): Bound[B] = bound match {
      case Finite(v) => Finite(f(v))
      case Bottom    => Bottom
      case Top       => Top
    }
  }

  implicit def encoder[V: Encoder]: Encoder[Bound[V]] =
    new Encoder[Bound[V]] {
      def apply(bound: Bound[V]) = bound match {
        case Bottom    => "<".asJson
        case Top       => ">".asJson
        case Finite(b) => b.asJson
      }
    }

  implicit def decoder[V: Decoder]: Decoder[Bound[V]] =
    Decoder.decodeString
      .emapTry {
        case "<" =>
          Try(Bottom)
        case ">" =>
          Try(Top)
        case other =>
          Try(Finite(implicitly[Decoder[V]].decodeJson(Json.fromString(other)).right.get))
      }
      // For backward compatibility we fallback to the generated decoder
      .or(deriveDecoder[Bound[V]])
}
