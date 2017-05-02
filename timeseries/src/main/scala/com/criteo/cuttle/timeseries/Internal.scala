package com.criteo.cuttle.timeseries

import com.criteo.cuttle._

import io.circe._

import cats.syntax.either._

import java.time.LocalDateTime

object Internal {

  implicit def jobEncoder[A <: Scheduling]: Encoder[Job[A]] =
    Encoder.encodeString.contramap(_.id)
  implicit def jobDecoder[A <: Scheduling](implicit jobs: Set[Job[A]]): Decoder[Job[A]] =
    Decoder.decodeString.map(id => jobs.find(_.id == id).get)

  implicit val dateTimeEncoder: Encoder[LocalDateTime] =
    Encoder.encodeString.contramap(_.toString)
  implicit val dateTimeDecoder: Decoder[LocalDateTime] =
    Decoder.decodeString.emap { s =>
      Either.catchNonFatal(LocalDateTime.parse(s)).leftMap(s => "LocalDateTime")
    }

}
