package com.criteo.cuttle

import java.sql.{Connection, ResultSet}

import scala.concurrent.Future

import cats.effect.IO
import com.mysql.cj.jdbc.PreparedStatement
import doobie.util.transactor.Transactor
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.FunSuite

import com.criteo.cuttle.ExecutionContexts.Implicits.sideEffectExecutionContext
import com.criteo.cuttle.ExecutionContexts._
import com.criteo.cuttle.Metrics.Prometheus

class ExecutorSpec extends FunSuite with TestScheduling {
  test("Executor should return metrics aggregated by job and tag") {
    val connection: Connection = {
      val mockConnection = mock(classOf[Connection])
      val statement = mock(classOf[PreparedStatement])
      val resultSet = mock(classOf[ResultSet])
      when(mockConnection.prepareStatement(any(classOf[String]))).thenReturn(statement)
      when(statement.executeQuery()).thenReturn(resultSet)
      mockConnection
    }

    val testExecutor = new Executor[TestScheduling](
      Seq.empty,
      xa = Transactor.fromConnection[IO](connection).copy(strategy0 = doobie.util.transactor.Strategy.void),
      logger,
      "test_project"
    )(RetryStrategy.ExponentialBackoffRetryStrategy)

    def fakeRun(job: Job[TestScheduling], finalState: FinishedExecutionStatus.Value): Unit = {
      val execution = buildExecutionForJob(job)
      testExecutor.initializeFinishedExecutionCounters(execution)
      testExecutor.updateFinishedExecutionCounters(execution, finalState)
    }

    fakeRun(fooJob, FinishedExecutionStatus.Success)
    fakeRun(fooJob, FinishedExecutionStatus.Success)
    fakeRun(untaggedJob, FinishedExecutionStatus.Success)
    fakeRun(fooBarJob, FinishedExecutionStatus.Success)
    fakeRun(untaggedJob, FinishedExecutionStatus.Failure)
    fakeRun(fooBarJob, FinishedExecutionStatus.Failure)
    testExecutor.initializeFinishedExecutionCounters(buildExecutionForJob(unfinishedJob))

    val metrics = Prometheus.serialize(
      testExecutor.getMetrics(Set("jobA"))(
        getStateAtomic = _ => {
          ((5, 1), 3, 2)
        },
        runningExecutions = Seq(
          buildExecutionForJob(fooJob) -> ExecutionStatus.ExecutionRunning,
          buildExecutionForJob(fooJob) -> ExecutionStatus.ExecutionRunning,
          buildExecutionForJob(fooJob) -> ExecutionStatus.ExecutionWaiting,
          buildExecutionForJob(fooBarJob) -> ExecutionStatus.ExecutionRunning,
          buildExecutionForJob(untaggedJob) -> ExecutionStatus.ExecutionRunning,
          buildExecutionForJob(untaggedJob) -> ExecutionStatus.ExecutionWaiting
        ),
        pausedExecutions = Seq(
          buildExecutionForJob(fooJob),
          buildExecutionForJob(fooBarJob),
          buildExecutionForJob(untaggedJob)
        ),
        failingExecutions = Seq(
          buildExecutionForJob(fooBarJob),
          buildExecutionForJob(fooBarJob),
          buildExecutionForJob(untaggedJob)
        )
      )
    )

    println(metrics)

    val expectedMetrics =
      """# HELP cuttle_scheduler_stat_count The number of jobs that we have in concrete states
         |# TYPE cuttle_scheduler_stat_count gauge
         |cuttle_scheduler_stat_count {type="running"} 5
         |cuttle_scheduler_stat_count {type="waiting"} 1
         |cuttle_scheduler_stat_count {type="paused"} 3
         |cuttle_scheduler_stat_count {type="failing"} 2
         |# HELP cuttle_scheduler_stat_count_by_tag The number of executions that we have in concrete states by tag
         |# TYPE cuttle_scheduler_stat_count_by_tag gauge
         |cuttle_scheduler_stat_count_by_tag {tag="bar", type="paused"} 1
         |cuttle_scheduler_stat_count_by_tag {tag="foo", type="waiting"} 1
         |cuttle_scheduler_stat_count_by_tag {tag="foo", type="running"} 3
         |cuttle_scheduler_stat_count_by_tag {tag="foo", type="paused"} 2
         |cuttle_scheduler_stat_count_by_tag {tag="bar", type="failing"} 2
         |cuttle_scheduler_stat_count_by_tag {tag="foo", type="failing"} 2
         |cuttle_scheduler_stat_count_by_tag {tag="bar", type="running"} 1
         |# HELP cuttle_scheduler_stat_count_by_job The number of executions that we have in concrete states by job
         |# TYPE cuttle_scheduler_stat_count_by_job gauge
         |cuttle_scheduler_stat_count_by_job {job="untagged_job", type="waiting"} 1
         |cuttle_scheduler_stat_count_by_job {job="foo_bar_job", type="running"} 1
         |cuttle_scheduler_stat_count_by_job {job="untagged_job", type="running"} 1
         |cuttle_scheduler_stat_count_by_job {job="foo_job", type="waiting"} 1
         |cuttle_scheduler_stat_count_by_job {job="foo_job", type="paused"} 1
         |cuttle_scheduler_stat_count_by_job {job="untagged_job", type="failing"} 1
         |cuttle_scheduler_stat_count_by_job {job="foo_job", type="running"} 2
         |cuttle_scheduler_stat_count_by_job {job="foo_bar_job", type="paused"} 1
         |cuttle_scheduler_stat_count_by_job {job="foo_bar_job", type="failing"} 2
         |cuttle_scheduler_stat_count_by_job {job="untagged_job", type="paused"} 1
         |# HELP cuttle_executions_total The number of finished executions that we have in concrete states by job and by tag
         |# TYPE cuttle_executions_total counter
         |cuttle_executions_total {type="failure", job_id="foo_job", tags="foo"} 0
         |cuttle_executions_total {type="success", job_id="foo_bar_job", tags="foo,bar"} 1
         |cuttle_executions_total {type="success", job_id="foo_job", tags="foo"} 2
         |cuttle_executions_total {type="failure", job_id="foo_bar_job", tags="foo,bar"} 1
         |cuttle_executions_total {type="failure", job_id="unfinished_job"} 0
         |cuttle_executions_total {type="success", job_id="unfinished_job"} 0
         |cuttle_executions_total {type="success", job_id="untagged_job"} 1
         |cuttle_executions_total {type="failure", job_id="untagged_job"} 1
         |""".stripMargin

    assert(metrics == expectedMetrics)
  }

  private def buildJob(jobId: String, tags: Set[Tag] = Set.empty): Job[TestScheduling] =
    Job(jobId, TestScheduling(), jobId, tags = tags) { implicit execution =>
      Future { Completed }(execution.executionContext)
    }

  private def buildExecutionForJob(job: Job[TestScheduling]): Execution[TestScheduling] =
    Execution[TestScheduling](
      id = java.util.UUID.randomUUID.toString,
      job = job,
      context = TestContext(),
      streams = new ExecutionStreams {
        override private[cuttle] def writeln(str: CharSequence): Unit = ???
      },
      platforms = Seq.empty,
      "foo-project"
    )

  private val fooTag = Tag("foo")
  private val barTag = Tag("bar")

  private val fooJob: Job[TestScheduling] = buildJob("foo_job", Set(fooTag))

  private val fooBarJob: Job[TestScheduling] = buildJob("foo_bar_job", Set(fooTag, barTag))

  private val untaggedJob: Job[TestScheduling] = buildJob("untagged_job")

  private val unfinishedJob: Job[TestScheduling] = buildJob("unfinished_job")
}
