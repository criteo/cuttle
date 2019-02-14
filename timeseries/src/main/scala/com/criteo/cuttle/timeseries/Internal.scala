package com.criteo.cuttle.timeseries

import com.criteo.cuttle._

import io.circe._
import io.circe.generic.semiauto._

import cats.syntax.either._

import java.time.Instant

import io.circe.generic.semiauto.deriveDecoder

private[timeseries] object Internal {

  implicit def jobEncoder[A <: Scheduling]: Encoder[Job[A]] =
    Encoder.encodeString.contramap(_.id)
  implicit def jobsDecoder[A <: Scheduling](implicit jobs: Set[Job[A]]): Decoder[Set[Job[A]]] =
    Decoder
      .decodeSet[Json]
      .map(_.collect(Function.unlift { id =>
        jobs.find(_.id == id)
      }))

  implicit val dateTimeEncoder: Encoder[Instant] =
    Encoder.encodeString.contramap(_.toString)
  implicit val dateTimeDecoder: Decoder[Instant] =
    Decoder.decodeString.emap { s =>
      Either.catchNonFatal(Instant.parse(s)).leftMap(s => "Instant")
    }
}

private[timeseries] object BackfillCreate {
  import Internal._

  implicit val decodeBackfillCreate: Decoder[BackfillCreate] = deriveDecoder[BackfillCreate]
}

private[timeseries] case class BackfillCreate(
  name: String,
  description: String,
  jobs: String,
  startDate: Instant,
  endDate: Instant,
  priority: Int
)

private[timeseries] object SortQuery {
  implicit val decodeExecutionsParams: Decoder[SortQuery] = deriveDecoder[SortQuery]
}

private[timeseries] case class SortQuery(
  column: String,
  order: String
) {
  val asc = order.toLowerCase == "asc"
}

private[timeseries] object ExecutionsQuery {
  implicit val decodeExecutionsParams: Decoder[ExecutionsQuery] = deriveDecoder[ExecutionsQuery]
}
private[timeseries] case class ExecutionsQuery(
  jobs: Set[String],
  sort: SortQuery,
  limit: Int,
  offset: Int
) {
  def jobIds(allIds: Set[String]) = if (jobs.isEmpty) allIds else jobs
}
