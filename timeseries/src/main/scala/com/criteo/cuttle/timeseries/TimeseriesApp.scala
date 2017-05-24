package com.criteo.cuttle.timeseries

import lol.http._
import lol.json._

import io.circe._
import io.circe.syntax._

import continuum._
import continuum.bound._

import java.time.Instant
import java.util.UUID

trait TimeSeriesApp { self: TimeSeriesScheduler =>

  import com.criteo.cuttle.JsonApi._

  implicit val intervalEncoder = new Encoder[Interval[Instant]] {
    override def apply(interval: Interval[Instant]) = {
      val Closed(start) = interval.lower.bound
      val Open(end) = interval.upper.bound
      Json.obj(
        "start" -> start.asJson,
        "end" -> end.asJson
      )
    }
  }

  override lazy val routes: PartialService = {
    case GET at "/api/timeseries" =>
      val (intervals, backfills) = state
      Ok(
        Json.obj(
          "jobs" -> Json.obj(
            intervals.toSeq
              .map {
                case (job, intervals) =>
                  job.id -> Json.fromValues(intervals.map(_.asJson))
              }
              .sortBy(_._1): _*
          ),
          "backfills" -> Json.fromValues(backfills.toSeq.sortBy(_.id).map { backfill =>
            Json.obj(
              "id" -> backfill.id.asJson,
              "start" -> backfill.start.asJson,
              "end" -> backfill.end.asJson,
              "jobs" -> Json.fromValues(backfill.jobs.map(_.id.asJson)),
              "priority" -> backfill.priority.asJson
            )
          })
        ))

    case POST at url"/api/timeseries/backfill?job=$id&startDate=$start&endDate=$end&priority=$priority" =>
      val job = this.state._1.keySet.find(_.id == id).get
      val startDate = Instant.parse(start)
      val endDate = Instant.parse(end)
      val backfillId = UUID.randomUUID().toString
      backfillJob(backfillId, job, startDate, endDate, priority.toInt)
      Ok(
        Json.obj(
          "id" -> backfillId.asJson
        ))
  }
}
