package com.criteo.cuttle.timeseries.intervals

import java.time.Duration

import com.criteo.cuttle.timeseries._
import com.criteo.cuttle.timeseries.JobState.{Done, Todo}
import com.criteo.cuttle.timeseries.TimeSeriesUtils.State
import com.criteo.cuttle.{Job, TestScheduling}
import de.sciss.fingertree.FingerTree
import org.scalatest.FunSuite

class TimeSeriesUtilsSpec extends FunSuite with TestScheduling {
  private val scheduling: TimeSeries = hourly(date"2017-03-25T02:00:00Z")
  private val jobA = Job("job_a", scheduling)(completed)
  private val jobB = Job("job_b", scheduling)(completed)

  test("clean overlapping state intervals") {
    val state: State = Map(
      jobA -> new IntervalMap.Impl(
        FingerTree(
          Interval(date"2017-03-25T01:00:00Z", date"2017-03-25T02:00:00Z") -> Done("v1"),
          Interval(date"2017-03-25T02:00:00Z", date"2017-03-25T04:00:00Z") -> Done("v2"),
          Interval(date"2017-03-25T04:00:00Z", date"2017-03-25T06:00:00Z") -> Todo(None),
          // Interval overlapping with previously defined interval
          Interval(date"2017-03-25T04:00:00Z", date"2017-03-25T05:00:00Z") -> Todo(None)
        )
      ),
      jobB -> new IntervalMap.Impl(
        FingerTree(
          Interval(date"2017-03-25T02:00:00Z", date"2017-03-25T03:00:00Z") -> Todo(None),
          Interval(date"2017-03-25T03:00:00Z", date"2017-03-25T04:00:00Z") -> Done("v2"),
          Interval(date"2017-03-25T04:30:00Z", date"2017-03-25T05:00:00Z") -> Todo(None),
          // Interval contiguous to the previous Done interval for jobB
          Interval(date"2017-03-25T04:00:00Z", date"2017-03-25T04:30:00Z") -> Done("v2")
        )
      )
    )

    TimeSeriesUtils.cleanTimeseriesState(state).foreach {
      case (job, intervalMap) =>
        job.id match {
          case jobA.id =>
            assert(
              intervalMap.toList.toSet.equals(
                Set(
                  (Interval(date"2017-03-25T01:00:00Z", date"2017-03-25T02:00:00Z"), Done("v1")),
                  (Interval(date"2017-03-25T02:00:00Z", date"2017-03-25T04:00:00Z"), Done("v2")),
                  (Interval(date"2017-03-25T04:00:00Z", date"2017-03-25T06:00:00Z"), Todo(None))
                )
              )
            )
          case jobB.id =>
            assert(
              intervalMap.toList.toSet.equals(
                Set(
                  (Interval(date"2017-03-25T02:00:00Z", date"2017-03-25T03:00:00Z"), Todo(None)),
                  (Interval(date"2017-03-25T03:00:00Z", date"2017-03-25T04:30:00Z"), Done("v2")),
                  (Interval(date"2017-03-25T04:30:00Z", date"2017-03-25T05:00:00Z"), Todo(None))
                )
              )
            )
        }
    }
  }

  test("validate workflow") {
    val j1 = jobAsWorkflow(Job("j1", hourly(date"2019-10-10T01:00:00Z"))(completed))
    val j2 = jobAsWorkflow(Job("j2", hourly(date"2019-10-10T02:00:00Z"))(completed))
    println(TimeSeriesUtils.validate(j2.dependsOn(j1)))
    assert(TimeSeriesUtils.validate(j2.dependsOn(j1)).isRight)
    assert(TimeSeriesUtils.validate(j1.dependsOn(j2)).isLeft)
    val timeOffset = TimeSeriesDependency(Duration.ofHours(1), Duration.ofHours(1))
    assert(TimeSeriesUtils.validate(j1.dependsOn((j2, timeOffset))).isRight)
  }
}
