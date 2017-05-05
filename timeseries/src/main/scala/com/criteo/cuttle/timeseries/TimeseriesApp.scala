package com.criteo.cuttle.timeseries

import lol.http._

import java.time.LocalDateTime
import java.util.UUID

trait TimeSeriesApp { self: TimeSeriesScheduler =>

  override lazy val routes: PartialService = {
    case GET at "/timeseries" =>
      Ok(s"TimeSeries: ${this.state}")

    case POST at url"/timeseries/backfill/$id/$start/$end/$priority" =>
      val job = this.state._1.keySet.find(_.id == id).get
      val startDate = LocalDateTime.parse(start)
      val endDate = LocalDateTime.parse(end)
      backfillJob(UUID.randomUUID().toString, job, startDate, endDate, priority.toInt)
      Ok("backfilled")
  }
}
