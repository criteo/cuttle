package com.criteo.cuttle.timeseries

import org.scalatest.FunSuite

import com.criteo.cuttle.timeseries.JobState.{Done, Todo}
import com.criteo.cuttle.timeseries.TimeSeriesUtils.State
import com.criteo.cuttle.timeseries.intervals.{Interval, IntervalMap}
import com.criteo.cuttle.{Job, TestScheduling}

class TimeSeriesUtilsSpec extends FunSuite with TestScheduling {
  private val scheduling: TimeSeries = hourly(date"2017-03-25T02:00:00Z")
  private val jobA = Job("job_a", scheduling)(completed)
  private val jobB = Job("job_b", scheduling)(completed)

  test("clean overlapping state intervals") {
    val state: State = Map(
      jobA -> IntervalMap(
        Interval(date"2017-03-25T01:00:00Z", date"2017-03-25T02:00:00Z") -> Done("v1"),
        Interval(date"2017-03-25T02:00:00Z", date"2017-03-25T04:00:00Z") -> Done("v2"),
        Interval(date"2017-03-25T04:00:00Z", date"2017-03-25T06:00:00Z") -> Todo(None),
        // Interval overlapping with previously defined interval
        Interval(date"2017-03-25T04:00:00Z", date"2017-03-25T05:00:00Z") -> Todo(None)
      ),
      jobB -> IntervalMap(
        Interval(date"2017-03-25T02:00:00Z", date"2017-03-25T03:00:00Z") -> Todo(None),
        Interval(date"2017-03-25T03:00:00Z", date"2017-03-25T04:00:00Z") -> Done("v2"),
        Interval(date"2017-03-25T04:30:00Z", date"2017-03-25T05:00:00Z") -> Todo(None),
        // Interval contiguous to the previous Done interval for jobB
        Interval(date"2017-03-25T04:00:00Z", date"2017-03-25T04:30:00Z") -> Done("v2")
      )
    )

    TimeSeriesUtils.cleanTimeseriesState(state).foreach { case (job, intervalMap) =>
      job.id match {
        case jobA.id =>
          assert(intervalMap.toList.toSet.equals(Set(
            (Interval(date"2017-03-25T01:00:00Z", date"2017-03-25T02:00:00Z"), Done("v1")),
            (Interval(date"2017-03-25T02:00:00Z", date"2017-03-25T04:00:00Z"), Done("v2")),
            (Interval(date"2017-03-25T04:00:00Z", date"2017-03-25T06:00:00Z"), Todo(None))
          )))
        case jobB.id =>
          assert(intervalMap.toList.toSet.equals(Set(
            (Interval(date"2017-03-25T02:00:00Z", date"2017-03-25T03:00:00Z"), Todo(None)),
            (Interval(date"2017-03-25T03:00:00Z", date"2017-03-25T04:30:00Z"), Done("v2")),
            (Interval(date"2017-03-25T04:30:00Z", date"2017-03-25T05:00:00Z"),Todo(None))
          )))
      }
    }
  }
}
