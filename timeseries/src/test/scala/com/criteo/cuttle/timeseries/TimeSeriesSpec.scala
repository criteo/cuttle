package com.criteo.cuttle.timeseries

import com.criteo.cuttle._
import org.scalatest.FunSuite
import java.time._
import java.time.temporal.ChronoUnit._

import scala.concurrent.Future

import intervals._
import Bound.{Finite, Top}

class TimeSeriesSpec extends FunSuite {
  val scheduling = hourly(date"2017-03-25T02:00:00Z")
  val job = (0 to 10).map(i => Job(i.toString, scheduling)(_ => Future.successful(())))
  val scheduler = TimeSeriesScheduler(logger)

  test("next") {
    val t = date"2017-03-25T00:00:00Z"
    val ts = (0 to 100).map(i => t.plus(i, HOURS))
    val state = Map(
      job(1) -> IntervalMap[Instant, JobState](Interval(Finite(ts(2)), Top) -> JobState.Todo(None))
    )
    val result = scheduler.jobsToRun(job(1), state, ts(5))
    assert(
      result ==
        (2 to 4).map { i =>
          (job(1), TimeSeriesContext(ts(i), ts(i + 1)))
        })
  }
}
