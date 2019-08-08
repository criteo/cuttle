package com.criteo.cuttle.timeseries

import java.time.Instant

import cats.syntax.either._
import io.circe._
import io.circe.generic.semiauto.deriveDecoder
import io.circe.java8.time._

import com.criteo.cuttle._
import com.criteo.cuttle.timeseries.intervals.Interval

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

  implicit val decodeBackfillCreate: Decoder[BackfillCreate] = deriveDecoder[BackfillCreate]
}

private[timeseries] case class BackfillCreate(
  name: String,
  description: String,
  jobs: List[String],
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

private[timeseries] object CalendarFocusQuery {
  implicit val decodeExecutionsParams: Decoder[CalendarFocusQuery] = deriveDecoder[CalendarFocusQuery]
}
private[timeseries] case class CalendarFocusQuery(
  jobs: Set[String],
  start: String,
  end: String
) {
  def jobIds(allIds: Set[String]) = if (jobs.isEmpty) allIds else jobs
}

private[timeseries] case class ExecutionDetails(jobExecutions: Seq[ExecutionLog], parentExecutions: Seq[ExecutionLog])

private[timeseries] object ExecutionDetails {
  implicit val executionDetailsEncoder: Encoder[ExecutionDetails] =
    Encoder.forProduct2("jobExecutions", "parentExecutions")(
      e => (e.jobExecutions, e.parentExecutions)
    )
}

private[timeseries] trait ExecutionPeriod {
  val period: Interval[Instant]
  val backfill: Boolean
  val aggregated: Boolean
  val version: String
}

private[timeseries] case class JobExecution(period: Interval[Instant],
                                            status: String,
                                            backfill: Boolean,
                                            version: String)
    extends ExecutionPeriod {
  override val aggregated: Boolean = false
}

private[timeseries] case class AggregatedJobExecution(period: Interval[Instant],
                                                      completion: Double,
                                                      error: Boolean,
                                                      backfill: Boolean,
                                                      version: String = "")
    extends ExecutionPeriod {
  override val aggregated: Boolean = true
}
