package com.criteo.cuttle.timeseries

import java.time.{Duration, Instant}

import scala.concurrent.Future

import org.scalatest.FunSuite

import com.criteo.cuttle.Auth
import com.criteo.cuttle.{Completed, Job, TestScheduling}
import com.criteo.cuttle.timeseries.JobState.{Done, Todo}
import com.criteo.cuttle.timeseries.TimeSeriesUtils.State
import com.criteo.cuttle.timeseries.intervals.{Bound, Interval, IntervalMap}
import com.criteo.cuttle.Utils.logger

class TimeSeriesSchedulerSpec extends FunSuite with TestScheduling {
  private val scheduling: TimeSeries = hourly(date"2017-03-25T02:00:00Z")
  private val testJob = Job("test_job", scheduling)(completed)

  private val parentScheduling: TimeSeries = hourly(date"2017-03-25T01:00:00Z")
  private val parentTestJob = Job("parent_test_job", parentScheduling)(completed)
  private val scheduler = TimeSeriesScheduler(logger)
  implicit val toto = Auth.User("toto")

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
    val (_, newBackfills, completedBackfills) =
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
    val (_, newBackfills, completedBackfills) = scheduler.collectCompletedJobs(
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
      jobsToRun.toSet.equals(
        Set(
          (testJob, TimeSeriesContext(date"2017-03-25T02:00:00Z", date"2017-03-25T03:00:00Z", None, "last_version")),
          (parentTestJob,
           TimeSeriesContext(date"2017-03-25T04:00:00Z", date"2017-03-25T05:00:00Z", None, "last_version"))
        )
      )
    )
  }

  val oneDayInterval = Interval(Instant.parse("2019-01-01T00:00:00Z"), Instant.parse("2019-01-02T00:00:00Z"))
  val start = Instant.parse("2019-01-01T00:00:00Z")

  test("Generate right n-hours periods") {

    def checkPeriod(period: Int) = {
      val allIntervals = nhourly(period, start).calendar.inInterval(oneDayInterval, 1).toList

      assert(allIntervals.size.equals(24 / period))

      assert(allIntervals.forall({
        case (lo, hi) =>
          Duration.between(lo, hi) == Duration.ofHours(period)
      }))

      assert(
        allIntervals
          .zip(allIntervals.drop(1))
          .forall({
            case (lo, hi) =>
              Duration.between(lo._2, hi._1) == Duration.ofHours(0)
          })
      )
    }

    List(1, 2, 3, 4, 6, 8, 12).map(i => checkPeriod(i))
  }

  test("NHourly can only be created for dividers of 24") {
    assertThrows[IllegalArgumentException](nhourly(-1, start))
    assertThrows[IllegalArgumentException](nhourly(0, start))

    List(1, 2, 3, 4, 6, 8, 12).foreach(i => {
      nhourly(i, start)
    })
    List(5, 7, 9, 10, 11, 13, 15, 16, 17, 18, 19, 20, 21, 22, 23).foreach(i => {
      assertThrows[IllegalArgumentException](nhourly(i, start))
    })

    assertThrows[IllegalArgumentException](nhourly(24, start))
    assertThrows[IllegalArgumentException](nhourly(25, start))
  }

  test("create backfills with fragmented versions") {
    val state: State = Map(
      testJob -> IntervalMap(
        Interval(date"2017-03-25T00:00:00Z", date"2017-03-25T12:00:00Z") -> Done("123"),
        Interval(date"2017-03-25T12:00:00Z", date"2017-03-27T00:00:00Z") -> Done("456"),
        Interval(date"2017-03-27T00:00:00Z", date"2017-03-28T00:00:00Z") -> Done("789")
      )
    )
    val backfills =
      scheduler.createBackfills(
        "lol",
        "",
        Set(testJob),
        state,
        date"2017-03-25T00:00:00Z",
        date"2017-03-26T00:00:00Z",
        0
      )
    assert(backfills.size == 1)
    assert(backfills(0).start == date"2017-03-25T00:00:00Z")
    assert(backfills(0).end == date"2017-03-26T00:00:00Z")
    assert(backfills(0).jobs.head == testJob)
    assert(backfills(0).jobs.last == testJob)
  }

  test("discard old versions in state") {
    val state: State = Map(
      testJob -> IntervalMap[Instant, JobState](
        Interval(date"2017-03-20T00:00:00Z", date"2017-03-25T00:00:00Z") -> Done("000"),
        Interval(date"2017-03-25T00:00:00Z", date"2017-03-25T12:00:00Z") -> Done("123"),
        Interval(date"2017-03-25T12:00:00Z", date"2017-03-25T14:00:00Z") -> Todo(None),
        Interval(date"2017-03-25T14:00:00Z", date"2017-03-27T00:00:00Z") -> Done("456"),
        Interval(date"2017-03-27T00:00:00Z", date"2017-03-28T00:00:00Z") -> Done("789"),
        Interval(Bound.Finite(date"2017-03-28T00:00:00Z"), Bound.Top) -> Todo(None)
      )
    )

    assert(
      scheduler.compressState(state, 20) ==
        Map(
          testJob -> IntervalMap[Instant, JobState](
            Interval(date"2017-03-20T00:00:00Z", date"2017-03-25T00:00:00Z") -> Done("000"),
            Interval(date"2017-03-25T00:00:00Z", date"2017-03-25T12:00:00Z") -> Done("123"),
            Interval(date"2017-03-25T12:00:00Z", date"2017-03-25T14:00:00Z") -> Todo(None),
            Interval(date"2017-03-25T14:00:00Z", date"2017-03-27T00:00:00Z") -> Done("456"),
            Interval(date"2017-03-27T00:00:00Z", date"2017-03-28T00:00:00Z") -> Done("789"),
            Interval(Bound.Finite(date"2017-03-28T00:00:00Z"), Bound.Top) -> Todo(None)
          )
        )
    )

    assert(
      scheduler.compressState(state, 3) ==
        Map(
          testJob -> IntervalMap[Instant, JobState](
            Interval(date"2017-03-20T00:00:00Z", date"2017-03-25T00:00:00Z") -> Done("old"),
            Interval(date"2017-03-25T00:00:00Z", date"2017-03-25T12:00:00Z") -> Done("123"),
            Interval(date"2017-03-25T12:00:00Z", date"2017-03-25T14:00:00Z") -> Todo(None),
            Interval(date"2017-03-25T14:00:00Z", date"2017-03-27T00:00:00Z") -> Done("456"),
            Interval(date"2017-03-27T00:00:00Z", date"2017-03-28T00:00:00Z") -> Done("789"),
            Interval(Bound.Finite(date"2017-03-28T00:00:00Z"), Bound.Top) -> Todo(None)
          )
        )
    )

    assert(
      scheduler.compressState(state, 2) ==
        Map(
          testJob -> IntervalMap[Instant, JobState](
            Interval(date"2017-03-20T00:00:00Z", date"2017-03-25T12:00:00Z") -> Done("old"),
            Interval(date"2017-03-25T12:00:00Z", date"2017-03-25T14:00:00Z") -> Todo(None),
            Interval(date"2017-03-25T14:00:00Z", date"2017-03-27T00:00:00Z") -> Done("456"),
            Interval(date"2017-03-27T00:00:00Z", date"2017-03-28T00:00:00Z") -> Done("789"),
            Interval(Bound.Finite(date"2017-03-28T00:00:00Z"), Bound.Top) -> Todo(None)
          )
        )
    )

    assert(
      scheduler.compressState(state, 1) ==
        Map(
          testJob -> IntervalMap[Instant, JobState](
            Interval(date"2017-03-20T00:00:00Z", date"2017-03-25T12:00:00Z") -> Done("old"),
            Interval(date"2017-03-25T12:00:00Z", date"2017-03-25T14:00:00Z") -> Todo(None),
            Interval(date"2017-03-25T14:00:00Z", date"2017-03-27T00:00:00Z") -> Done("old"),
            Interval(date"2017-03-27T00:00:00Z", date"2017-03-28T00:00:00Z") -> Done("789"),
            Interval(Bound.Finite(date"2017-03-28T00:00:00Z"), Bound.Top) -> Todo(None)
          )
        )
    )

    assert(
      scheduler.compressState(state, 0) ==
        Map(
          testJob -> IntervalMap[Instant, JobState](
            Interval(date"2017-03-20T00:00:00Z", date"2017-03-25T12:00:00Z") -> Done("old"),
            Interval(date"2017-03-25T12:00:00Z", date"2017-03-25T14:00:00Z") -> Todo(None),
            Interval(date"2017-03-25T14:00:00Z", date"2017-03-28T00:00:00Z") -> Done("old"),
            Interval(Bound.Finite(date"2017-03-28T00:00:00Z"), Bound.Top) -> Todo(None)
          )
        )
    )

  }

}
