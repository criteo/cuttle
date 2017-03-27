package com.criteo.langoustinepp

import org.scalatest.FunSuite
import java.time._
import java.time.temporal.ChronoUnit._
import codes.reactive.scalatime._
import continuum.{Interval, IntervalSet}
import continuum.bound._

class TimeSeriesSpec extends FunSuite {
  implicit val defaultDuration: Duration = 0.hours
  val job = (0 to 10).map(i => Job(i.toString, TimeSeriesScheduling(Hourly)))
  val scheduler = new TimeSeriesScheduler(job(1) dependsOn job(0))

  import scheduler.dateTimeOrdering

  test("split on hours") {
    val result = scheduler.split(
      LocalDateTime.of(2017, 3, 26, 0, 15),
      LocalDateTime.of(2017, 3, 26, 3, 0),
      ZoneId.of("Europe/Paris"), HOURS)
    assert(result.toList == List(
      (LocalDateTime.of(2017, 3, 26, 1, 0), LocalDateTime.of(2017, 3, 26, 2, 0)),
      (LocalDateTime.of(2017, 3, 26, 2, 0), LocalDateTime.of(2017, 3, 26, 3, 0))
    ))
  }
  test ("split on days") {
    val result = scheduler.split(
      LocalDateTime.of(2017, 3, 25, 1, 0),
      LocalDateTime.of(2017, 3, 28, 0, 0),
      ZoneId.of("Europe/Paris"), DAYS)
    assert(result.toList == List(
      (LocalDateTime.of(2017, 3, 25, 23, 0), LocalDateTime.of(2017, 3, 26, 22, 0)),
      (LocalDateTime.of(2017, 3, 26, 22, 0), LocalDateTime.of(2017, 3, 27, 22, 0))
    ))
  }
  test ("next") {
    val t = LocalDateTime.of(2017, 3, 25, 0, 0)
    val ts = (1 to 100).map(i => t.plus(i, HOURS))
    val result = scheduler.next(Map(
      job(0) -> IntervalSet(Interval.closedOpen(ts(1), ts(5)))
    ))
    assert(result ==
      (1 to 4).map { i => (job(1), (ts(i), ts(i+1))) }
    )
  }
}
