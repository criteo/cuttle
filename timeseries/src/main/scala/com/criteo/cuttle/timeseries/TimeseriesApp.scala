package com.criteo.cuttle.timeseries

import lol.http._

trait TimeSeriesApp { self: TimeSeriesScheduler =>

  override lazy val routes: PartialService = {
    case GET at "/timeseries" =>
      Ok(s"TimeSeries: ${this.state}")
  }
}
