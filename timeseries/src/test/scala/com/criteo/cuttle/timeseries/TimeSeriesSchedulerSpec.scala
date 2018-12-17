package com.criteo.cuttle.timeseries

import java.time.Duration

import scala.concurrent.Future

import org.scalatest.FunSuite

import com.criteo.cuttle.{logger, Completed, Job, TestScheduling}
import com.criteo.cuttle.timeseries.JobState.{Done, Todo}
import com.criteo.cuttle.timeseries.TimeSeriesUtils.State
import com.criteo.cuttle.timeseries.intervals.{Interval, IntervalMap}

class TimeSeriesSchedulerSpec extends FunSuite with TestScheduling {
  private val scheduling: TimeSeries = hourly(date"2017-03-25T02:00:00Z")
  private val testJob = Job("test_job", scheduling)(completed)

  private val parentScheduling: TimeSeries = hourly(date"2017-03-25T01:00:00Z")
  private val parentTestJob = Job("parent_test_job", parentScheduling)(completed)
  private val scheduler = TimeSeriesScheduler(logger)

  private val backfill = Backfill("some-id",
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
      testJob -> IntervalMap(
        Interval(date"2017-03-25T00:00:00Z", date"2017-03-25T01:00:00Z") -> Done(""),
        // Backfill completed on the last 3 hours of the backfill period, first hour not yet done
        Interval(date"2017-03-25T01:00:00Z", date"2017-03-25T02:00:00Z") -> Todo(Some(backfill)),
        Interval(date"2017-03-25T02:00:00Z", date"2017-03-25T05:00:00Z") -> Done("")
      )
    )
    val (stateSnapshot, newBackfills, completedBackfills) =
      scheduler.collectCompletedJobs(state, Set(backfill), completed = Set.empty)
    assert(newBackfills.equals(Set(backfill)))
    assert(completedBackfills.isEmpty)
  }

  test("complete backfills") {
    val state: State = Map(
      testJob -> IntervalMap(
        Interval(date"2017-03-25T00:00:00Z", date"2017-03-25T01:00:00Z") -> Done(""),
        Interval(date"2017-03-25T02:00:00Z", date"2017-03-25T05:00:00Z") -> Done("")
      )
    )
    val (stateSnapshot, newBackfills, completedBackfills) = scheduler.collectCompletedJobs(
      state,
      Set(backfill),
      completed = Set(
        // Another non backfilled execution completed on the period where 'backfill' is still running
        (testJob,
         TimeSeriesContext(date"2017-03-25T01:00:00Z", date"2017-03-25T02:00:00Z"),
         Future.successful(Completed))
      )
    )
    assert(completedBackfills.equals(Set(backfill)))
    assert(newBackfills.isEmpty)
  }

  test("complete regular execution while backfill is still running") {
    // Edge-case not possible since it is not possible to run a backfill of a job on a period where that job did not complete
  }

  test("identify jobs to do") {
    val state: State = Map(
      parentTestJob -> IntervalMap(
        Interval(date"2017-03-25T01:00:00Z", date"2017-03-25T02:00:00Z") -> Done("v1"),
        Interval(date"2017-03-25T02:00:00Z", date"2017-03-25T04:00:00Z") -> Done("v2"),
        Interval(date"2017-03-25T04:00:00Z", date"2017-03-25T05:00:00Z") -> Todo(None)
      ),
      testJob -> IntervalMap(
        Interval(date"2017-03-25T02:00:00Z", date"2017-03-25T03:00:00Z") -> Todo(None),
        Interval(date"2017-03-25T03:00:00Z", date"2017-03-25T04:00:00Z") -> Done("v2"),
        Interval(date"2017-03-25T04:00:00Z", date"2017-03-25T05:00:00Z") -> Todo(None)
      )
    )

    val jobsToRun = scheduler.jobsToRun(
      (testJob dependsOn parentTestJob)(TimeSeriesDependency(Duration.ofHours(-1), Duration.ofHours(0))),
      state,
      date"2017-03-25T05:00:00Z",
      "last_version"
    )
    assert(
      jobsToRun.toSet.equals(Set(
        (testJob, TimeSeriesContext(date"2017-03-25T02:00:00Z", date"2017-03-25T03:00:00Z", None, "last_version")),
        (parentTestJob, TimeSeriesContext(date"2017-03-25T04:00:00Z", date"2017-03-25T05:00:00Z", None, "last_version"))
      )))
  }
}
