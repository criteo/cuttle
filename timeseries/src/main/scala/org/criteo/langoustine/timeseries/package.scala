package org.criteo.langoustine

import java.time.{ LocalDateTime }
import codes.reactive.scalatime._

package object timeseries {

  implicit val defaultDependencyDescriptor: TimeSeriesDependency =
    TimeSeriesDependency(0.hours)

  def hourly(start: LocalDateTime) = TimeSeriesScheduling(grid = Hourly, start)

}
