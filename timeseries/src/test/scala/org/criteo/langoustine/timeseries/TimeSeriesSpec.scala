package org.criteo.langoustine.timeseries

import org.criteo.langoustine._

import org.scalatest.FunSuite

import java.time._
import java.time.temporal.ChronoUnit._

import scala.concurrent.{Future}

import continuum.{Interval, IntervalSet}

class TimeSeriesSpec extends FunSuite {
  val scheduling = hourly(LocalDateTime.of(2017, 3, 25, 2, 0))
  val job = (0 to 10).map(i => Job(i.toString, scheduling)(_ => Future.successful(())))
  val scheduler = TimeSeriesScheduler()

  import scheduler.dateTimeOrdering

  test("split on hours") {
    val result = scheduler.split(LocalDateTime.of(2017, 3, 26, 0, 15),
                                 LocalDateTime.of(2017, 3, 26, 3, 0),
                                 ZoneId.of("Europe/Paris"),
                                 HOURS,
                                 1)
    val midnight = LocalDateTime.of(2017, 3, 26, 0, 0)
    assert(
      result.toList == List(1, 2).map(i => TimeSeriesContext(midnight.plus(i, HOURS), midnight.plus(i + 1, HOURS))))
  }
  test("split on days") {
    val result = scheduler.split(LocalDateTime.of(2017, 3, 25, 1, 0),
                                 LocalDateTime.of(2017, 3, 28, 0, 0),
                                 ZoneId.of("Europe/Paris"),
                                 DAYS,
                                 1)
    val midnightParis = LocalDateTime.of(2017, 3, 25, 23, 0)
    assert(
      result.toList == List(
        TimeSeriesContext(midnightParis, midnightParis.plus(23, HOURS)),
        TimeSeriesContext(midnightParis.plus(23, HOURS), midnightParis.plus(23 + 24, HOURS))
      ))
  }
  test("split with maxPeriods") {
    val midnight = LocalDateTime.of(2017, 1, 1, 0, 0)
    val result = scheduler.split(midnight, midnight.plus(5, HOURS), ZoneId.of("UTC"), HOURS, 2)
    assert(
      result.toList == List(
        TimeSeriesContext(midnight, midnight.plus(2, HOURS)),
        TimeSeriesContext(midnight.plus(2, HOURS), midnight.plus(4, HOURS)),
        TimeSeriesContext(midnight.plus(4, HOURS), midnight.plus(5, HOURS))
      ))
  }
  test("next") {
    val t = LocalDateTime.of(2017, 3, 25, 0, 0)
    val ts = (0 to 100).map(i => t.plus(i, HOURS))
    val state = Map(
      job(1) -> IntervalSet.empty[LocalDateTime]
    )
    val result = scheduler.next(job(1), state, Set.empty, IntervalSet(Interval.closedOpen(ts(1), ts(5))))
    assert(
      result ==
        (2 to 4).map { i =>
          (job(1), TimeSeriesContext(ts(i), ts(i + 1)))
        })
  }
}
