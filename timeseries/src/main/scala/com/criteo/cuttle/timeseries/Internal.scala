package com.criteo.cuttle.timeseries

import com.criteo.cuttle._

import io.circe._

import cats.syntax.either._

import java.time.Instant

object Internal {

  implicit def jobEncoder[A <: Scheduling]: Encoder[Job[A]] =
    Encoder.encodeString.contramap(_.id)
  implicit def jobDecoder[A <: Scheduling](implicit jobs: Set[Job[A]]): Decoder[Job[A]] =
    Decoder.decodeString.map(id => jobs.find(_.id == id).get)

  implicit val dateTimeEncoder: Encoder[Instant] =
    Encoder.encodeString.contramap(_.toString)
  implicit val dateTimeDecoder: Decoder[Instant] =
    Decoder.decodeString.emap { s =>
      Either.catchNonFatal(Instant.parse(s)).leftMap(s => "Instant")
    }

}
