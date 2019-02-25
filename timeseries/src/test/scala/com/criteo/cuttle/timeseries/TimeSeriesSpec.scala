package com.criteo.cuttle.timeseries

import java.time._
import java.time.temporal.ChronoUnit._

import org.scalatest.FunSuite

import com.criteo.cuttle._
import com.criteo.cuttle.timeseries.intervals.Bound.{Finite, Top}
import com.criteo.cuttle.timeseries.intervals._
import com.criteo.cuttle.Utils.logger

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
    val result = scheduler.jobsToRun(job(1), state, ts(5), "test_version")
    assert(
      result ==
        (2 to 4).map { i =>
          (job(1), TimeSeriesContext(ts(i), ts(i + 1), projectVersion = "test_version"))
        })
  }

  test("it should validate empty workflow") {
    val workflow = Workflow.empty

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

  test("it should validate workflow without cycles (one parent with many children)") {
    val job1: Vector[Job[TimeSeries]] =
      Vector.tabulate(10)(i => Job(java.util.UUID.randomUUID.toString, scheduling)(completed))
    val workflow = (0 to 8).map(i => job1(i) dependsOn job1(9)).reduce(_ and _)

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
    assert(
      validationRes.left.get === List(
        "Workflow has at least one cycle",
        "{1,badJob} form a cycle",
        "Job [0] starts at [2017-03-25T02:00:00Z] before his parent [badJob] at [2117-03-25T02:00:00Z]",
        "Job [1] starts at [2017-03-25T02:00:00Z] before his parent [badJob] at [2117-03-25T02:00:00Z]"
      ),
      "errors messages are bad"
    )
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

  test("it should create backfills on valid subperiods") {
    val job1 = Job("job1", hourly(date"2117-03-25T02:00:00Z"))(completed)
    val job2 = Job("job2", hourly(date"2117-03-24T02:00:00Z"))(completed)

    // hours 1 2 3 4 5 6 7
    // job1  VVVVVV--VVVVV
    // job2  --VVVV----VVV
    // V means that the job was done on that period. Periods are indivisible.
    // Backfill requested from hour 2 to 7
    // Backfills on periods overlapping valid calendar periods are automatically resolved when the actual backfill is run.
    val jobStates = Map[TimeSeriesUtils.TimeSeriesJob, IntervalMap[Instant, JobState]](
      job1 -> IntervalMap[Instant, JobState](
        List(
          (Interval(date"2117-03-25T01:00:00Z", date"2117-03-25T04:00:00Z"), JobState.Done("test_version")),
          (Interval(date"2117-03-25T04:00:00Z", date"2117-03-25T05:00:00Z"), JobState.Todo(None)),
          (Interval(date"2117-03-25T05:00:00Z", date"2117-03-25T07:00:00Z"), JobState.Done("test_version"))
        ): _*),
      job2 -> IntervalMap[Instant, JobState](
        List(
          (Interval(date"2117-03-25T02:00:00Z", date"2117-03-25T04:00:00Z"), JobState.Done("test_version")),
          (Interval(date"2117-03-25T04:00:00Z", date"2117-03-25T06:00:00Z"), JobState.Todo(None)),
          (Interval(date"2117-03-25T06:00:00Z", date"2117-03-25T07:00:00Z"), JobState.Done("test_version"))
        ): _*)
    )

    val backfills = scheduler
      .createBackfills("mock_backfill",
                       "Test backfill",
                       Set(job1, job2),
                       jobStates,
                       start = date"2117-03-25T02:00:00Z",
                       end = date"2117-03-25T07:00:00Z",
                       priority = 0)(Auth.User("test_user_id"))
      .sortBy(_.start)

    assert(backfills.size === 3)
    val firstBackfill = (backfills(0).start, backfills(0).end, backfills(0).jobs)
    val secondBackfill = (backfills(1).start, backfills(1).end, backfills(1).jobs)
    val thirdBackfill = (backfills(2).start, backfills(2).end, backfills(2).jobs)
    backfills.forall { backfill =>
      val validationErrors = scheduler.validateBackfill(backfill, jobStates)
      validationErrors.isEmpty
    }
    assert(
      firstBackfill.equals(
        (date"2117-03-25T02:00:00Z", date"2117-03-25T04:00:00Z", Set(job1, job2))
      ))
    assert(
      secondBackfill.equals(
        (date"2117-03-25T05:00:00Z", date"2117-03-25T06:00:00Z", Set(job1))
      ))
    assert(
      thirdBackfill.equals(
        (date"2117-03-25T06:00:00Z", date"2117-03-25T07:00:00Z", Set(job1, job2))
      ))
  }

  test("should parse JobState.Done without projectVersion") {
    import io.circe.parser._

    val state = """{
      "Done": {}
    }"""

    val parseDone = parse(state).right.flatMap(json => json.as[JobState.Done])
    assert(parseDone.isRight)
    assert(parseDone.right.get == JobState.Done(projectVersion = "no-version"))
  }

  test("should parse JobState without projectVersion") {
    import io.circe.parser._

    implicit val jobs: Set[Job[TimeSeries]] = Set.empty
    implicit val backfills: List[Backfill] = List.empty

    val state = """{
      "Done": {}
    }"""

    val parseState = parse(state).right.flatMap(json => json.as[JobState])
    assert(parseState.isRight)
    assert(parseState.right.get == JobState.Done(projectVersion = "no-version"))
  }

  test("should parse JobState with projectVersion") {
    import io.circe.parser._

    implicit val jobs: Set[Job[TimeSeries]] = Set.empty
    implicit val backfills: List[Backfill] = List.empty

    val state = """{
      "Done": { "projectVersion" : "version" }
    }"""

    val parseState = parse(state).right.flatMap(_.as[JobState])
    assert(parseState.isRight)
    assert(parseState.right.get == JobState.Done(projectVersion = "version"))
  }
}
