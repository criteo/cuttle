package org.criteo.langoustine.timeseries

import org.criteo.langoustine._

import org.scalatest.FunSuite

import java.time._
import java.time.temporal.ChronoUnit._

import scala.concurrent.{ Future }

import continuum.{Interval, IntervalSet}

class TimeSeriesSpec extends FunSuite {
  val scheduling = hourly(LocalDateTime.of(2017, 3, 25, 2, 0))
  val job = (0 to 10).map(i => Job(i.toString, scheduling)(_ => Future.successful(())))
  val scheduler = new TimeSeriesScheduler(job(1) dependsOn job(0), Executor(Nil))

  import scheduler.dateTimeOrdering

  test("split on hours") {
    val result = scheduler.split(
      LocalDateTime.of(2017, 3, 26, 0, 15),
      LocalDateTime.of(2017, 3, 26, 3, 0),
      ZoneId.of("Europe/Paris"), HOURS)
    val midnight = LocalDateTime.of(2017, 3, 26, 0, 0)
    assert(result.toList == List(1, 2).map(i =>
        TimeSeriesContext(midnight.plus(i, HOURS), midnight.plus(i+1, HOURS)))
      )
  }
  test ("split on days") {
    val result = scheduler.split(
      LocalDateTime.of(2017, 3, 25, 1, 0),
      LocalDateTime.of(2017, 3, 28, 0, 0),
      ZoneId.of("Europe/Paris"), DAYS)
    val midnightParis = LocalDateTime.of(2017, 3, 25, 23, 0)
    assert(result.toList == List(
      TimeSeriesContext(midnightParis, midnightParis.plus(23, HOURS)),
      TimeSeriesContext(midnightParis.plus(23, HOURS), midnightParis.plus(23+24, HOURS))
    ))
  }
  test ("next") {
    val t = LocalDateTime.of(2017, 3, 25, 0, 0)
    val ts = (0 to 100).map(i => t.plus(i, HOURS))
    val result = scheduler.next(Map(
      job(0) -> IntervalSet(Interval.closedOpen(ts(1), ts(5)))
    ))
    assert(result ==
      (2 to 4).map { i => (job(1), TimeSeriesContext(ts(i), ts(i+1))) }
    )
  }
}
