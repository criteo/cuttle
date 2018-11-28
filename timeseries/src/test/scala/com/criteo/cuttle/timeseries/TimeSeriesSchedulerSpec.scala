package com.criteo.cuttle.timeseries

import scala.concurrent.Future

import org.scalatest.FunSuite

import com.criteo.cuttle.{Completed, Job, TestScheduling, logger}
import com.criteo.cuttle.timeseries.JobState.{Done, Todo}
import com.criteo.cuttle.timeseries.TimeSeriesUtils.State
import com.criteo.cuttle.timeseries.intervals.{Interval, IntervalMap}


class TimeSeriesSchedulerSpec extends FunSuite with TestScheduling  {
  private val scheduling: TimeSeries = hourly(date"2017-03-25T02:00:00Z")
  private val testJob = Job("test_job", scheduling)(completed)
  private val scheduler = TimeSeriesScheduler(logger)

  private val backfill = Backfill(
    "some-id",
    date"2017-03-25T01:00:00Z",
    date"2017-03-25T05:00:00Z",
    Set(testJob),
    priority = 0,
    name = "backfill",
    description = "",
    status = "RUNNING",
    createdBy = "")

  test("identity new backfills") {
    val state: State = Map(
      testJob ->  IntervalMap(
        Interval(date"2017-03-25T00:00:00Z", date"2017-03-25T01:00:00Z") -> Done(""),
        // Backfill completed on the last 3 hours of the backfill period, first hour not yet done
        Interval(date"2017-03-25T01:00:00Z", date"2017-03-25T02:00:00Z") -> Todo(Some(backfill)),
        Interval(date"2017-03-25T02:00:00Z", date"2017-03-25T05:00:00Z") -> Done("")
      )
    )
    val (stateSnapshot, newBackfills, completedBackfills) = scheduler.collectCompletedJobs(state, Set(backfill), completed = Set.empty)
    assert(newBackfills.equals(Set(backfill)))
    assert(completedBackfills.isEmpty)
  }

  test("complete backfills") {
    val state: State = Map(
      testJob ->  IntervalMap(
        Interval(date"2017-03-25T00:00:00Z", date"2017-03-25T01:00:00Z") -> Done(""),
        Interval(date"2017-03-25T02:00:00Z", date"2017-03-25T05:00:00Z") -> Done("")
      )
    )
    val (stateSnapshot, newBackfills, completedBackfills) = scheduler.collectCompletedJobs(
      state,
      Set(backfill),
      completed = Set(
        // Another non backfilled execution completed on the period where 'backfill' is still running
        (testJob, TimeSeriesContext(date"2017-03-25T01:00:00Z", date"2017-03-25T02:00:00Z"), Future.successful(Completed))
      ))
    assert(completedBackfills.equals(Set(backfill)))
    assert(newBackfills.isEmpty)
  }

  test("complete regular execution while backfill is still running") {
    // Edge-case not possible since it is not possible to run a backfill of a job on a period where that job did not complete
  }
}