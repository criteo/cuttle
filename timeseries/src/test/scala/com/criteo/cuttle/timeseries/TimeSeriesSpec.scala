package com.criteo.cuttle.timeseries

import java.time._
import java.time.temporal.ChronoUnit._

import org.scalatest.FunSuite

import com.criteo.cuttle._
import com.criteo.cuttle.timeseries.intervals.Bound.{Finite, Top}
import com.criteo.cuttle.timeseries.intervals._

class TimeSeriesSpec extends FunSuite with TestScheduling {
  val scheduling: TimeSeries = hourly(date"2017-03-25T02:00:00Z")
  val job: Vector[Job[TimeSeries]] = Vector.tabulate(4)(i => Job(i.toString, scheduling)(completed))
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

  test("it should validate empty workflow") {
    val workflow = Workflow.empty[TimeSeries]

    assert(TimeSeriesUtils.validate(workflow).isRight, "workflow is not valid")
  }

  test("it should validate unit workflow") {
    val workflow = job(0)

    assert(TimeSeriesUtils.validate(workflow).isRight, "workflow is not valid")
  }

  test("it should validate workflow without cycles and valid start dates") {
    val workflow = job(0) dependsOn job(1) dependsOn job(2)

    assert(TimeSeriesUtils.validate(workflow).isRight, "workflow is not valid")
  }

  test("it shouldn't validate cyclic workflow") {
    val workflow = job(0) dependsOn job(1) dependsOn job(2) dependsOn job(0)

    assert(TimeSeriesUtils.validate(workflow).isLeft, "workflow passed a validation of cycle presence")
  }

  test("it should validate workflow with correct start dates") {
    val oldJob = Job("badJob", hourly(date"2016-03-25T02:00:00Z"))(completed)
    val workflow = job(0) dependsOn oldJob

    assert(TimeSeriesUtils.validate(workflow).isRight, "workflow didn't pass start date validation")
  }

  test("it shouldn't validate workflow with incorrect start dates of jobs") {
    val badJob = Job("badJob", hourly(date"2117-03-25T02:00:00Z"))(completed)
    val workflow = job(0) dependsOn (job(1) and job(2)) dependsOn badJob

    val validationRes = TimeSeriesUtils.validate(workflow)
    assert(validationRes.isLeft, "workflow passed start date validation")
    assert(
      validationRes.left.get.toSet === Set(
        "Job [2] starts at [2017-03-25T02:00:00Z] before his parent [badJob] at [2117-03-25T02:00:00Z]",
        "Job [1] starts at [2017-03-25T02:00:00Z] before his parent [badJob] at [2117-03-25T02:00:00Z]"
      ),
      "errors messages are bad"
    )
  }

  test("it shouldn't validate cyclic workflow with incorrect start dates of jobs") {
    val badJob = Job("badJob", hourly(date"2117-03-25T02:00:00Z"))(completed)
    val workflow = job(0) dependsOn badJob dependsOn job(1) dependsOn badJob

    val validationRes = TimeSeriesUtils.validate(workflow)
    assert(validationRes.isLeft, "workflow passed start date validation")
    assert(validationRes.left.get === List(
      "Workflow has at least one cycle",
      "Job [0] starts at [2017-03-25T02:00:00Z] before his parent [badJob] at [2117-03-25T02:00:00Z]",
      "Job [1] starts at [2017-03-25T02:00:00Z] before his parent [badJob] at [2117-03-25T02:00:00Z]"
    ), "errors messages are bad")
  }

  test("it shouldn't validate a workflow that contains jobs with same ids") {
    val id = "badJob"
    val badJob = Job(id, hourly(date"2117-03-25T02:00:00Z"))(completed)
    val badJobClone = Job(id, hourly(date"2117-03-24T02:00:00Z"))(completed)
    val workflowParentChild = badJob dependsOn badJobClone
    val workflowSiblings = badJob and badJobClone

    val validationParentChild = TimeSeriesUtils.validate(workflowParentChild)
    assert(validationParentChild.isLeft, "it means that workflow passed duplicate id validation")
    assert(validationParentChild.left.get === List(s"Id badJob is used by more than 1 job"),
           "it means that errors messages are bad")

    val validationSiblings = TimeSeriesUtils.validate(workflowSiblings)
    assert(validationSiblings.isLeft, "it means that workflow passed duplicate id validation")
    assert(validationSiblings.left.get === List(s"Id badJob is used by more than 1 job"),
           "it means that errors messages are bad")
  }
}
