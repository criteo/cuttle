package com.criteo.cuttle.timeseries

import lol.http._
import lol.json._

import io.circe._
import io.circe.syntax._

import java.time.LocalDateTime
import java.util.UUID

trait TimeSeriesApp { self: TimeSeriesScheduler =>

  override lazy val routes: PartialService = {
    case GET at "/api/timeseries" =>
      Ok(s"TimeSeries: ${this.state}")

    case POST at url"/api/timeseries/backfill?job=$id&startDate=$start&endDate=$end&priority=$priority" =>
      val job = this.state._1.keySet.find(_.id == id).get
      val startDate = LocalDateTime.parse(start)
      val endDate = LocalDateTime.parse(end)
      val backfillId = UUID.randomUUID().toString
      backfillJob(backfillId, job, startDate, endDate, priority.toInt)
      Ok(Json.obj(
        "id" -> backfillId.asJson
      ))
  }
}
