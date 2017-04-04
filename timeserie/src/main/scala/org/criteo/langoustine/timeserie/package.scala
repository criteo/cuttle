package org.criteo.langoustine

import java.time.{ Duration }
import codes.reactive.scalatime._

package object timeserie {

  implicit val defaultDuration: Duration = 0.hours

  def hourly() = TimeSeriesScheduling(Hourly)

}