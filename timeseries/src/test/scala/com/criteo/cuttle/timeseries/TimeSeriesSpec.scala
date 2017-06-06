package com.criteo.cuttle.timeseries

import java.time.ZoneOffset.UTC

import com.criteo.cuttle._
import org.scalatest.FunSuite
import java.time._
import java.time.temporal.ChronoUnit._

import scala.concurrent.Future
import continuum.{Interval, IntervalSet}

class TimeSeriesSpec extends FunSuite {
  val scheduling = hourly(date"2017-03-25T02:00:00Z")
  val job = (0 to 10).map(i => Job(i.toString, scheduling)(_ => Future.successful(())))
  val scheduler = TimeSeriesScheduler()

  import TimeSeriesUtils.dateTimeOrdering

  test("split on hours") {
    val result =
      scheduler
        .split(date"2017-03-26T00:15:00Z", date"2017-03-26T03:00:00Z", ZoneId.of("Europe/Paris"), HOURS, true, 1)
    val midnight = date"2017-03-26T00:00:00Z"
    assert(
      result.toList == List(1, 2).map(i => TimeSeriesContext(midnight.plus(i, HOURS), midnight.plus(i + 1, HOURS))))
  }

  test("split on days") {
    val result =
      scheduler.split(date"2017-03-25T01:00:00Z", date"2017-03-28T00:00:00Z", ZoneId.of("Europe/Paris"), DAYS, true, 1)
    val midnightParis = date"2017-03-25T23:00:00Z"
    assert(
      result.toList == List(
        TimeSeriesContext(midnightParis, midnightParis.plus(23, HOURS)),
        TimeSeriesContext(midnightParis.plus(23, HOURS), midnightParis.plus(23 + 24, HOURS))
      ))
  }

  test("split with maxPeriods") {
    val midnight = date"2017-01-01T00:00:00Z"
    val result =
      scheduler.split(start = midnight, end = midnight.plus(5, HOURS), tz = UTC, unit = HOURS, true, maxPeriods = 2)
    assert(
      result.toList == List(
        TimeSeriesContext(midnight, midnight.plus(2, HOURS)),
        TimeSeriesContext(midnight.plus(2, HOURS), midnight.plus(4, HOURS)),
        TimeSeriesContext(midnight.plus(4, HOURS), midnight.plus(5, HOURS))
      ))
  }

  test("next") {
    val t = date"2017-03-25T00:00:00Z"
    val ts = (0 to 100).map(i => t.plus(i, HOURS))
    val state = Map(
      job(1) -> IntervalSet.empty[Instant]
    )
    val result = scheduler.next(job(1), state, Set.empty, Set.empty, IntervalSet(Interval.closedOpen(ts(1), ts(5))))
    assert(
      result ==
        (2 to 4).map { i =>
          (job(1), TimeSeriesContext(ts(i), ts(i + 1)))
        })
  }
}
