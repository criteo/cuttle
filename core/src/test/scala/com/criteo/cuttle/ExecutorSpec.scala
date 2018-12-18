package com.criteo.cuttle

import java.sql.{Connection, ResultSet}

import scala.concurrent.Future
import cats.effect.IO
import com.mysql.cj.jdbc.PreparedStatement
import doobie.util.transactor.Transactor
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.FunSuite
import com.criteo.cuttle.ThreadPools.Implicits.sideEffectThreadPool
import com.criteo.cuttle.ThreadPools.Implicits.sideEffectContextShift
import com.criteo.cuttle.ThreadPools._

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
      xa = Transactor.fromConnection[IO](connection, sideEffectThreadPool).copy(strategy0 = doobie.util.transactor.Strategy.void),
      logger,
      "project_name",
      "test_version"
    )(RetryStrategy.ExponentialBackoffRetryStrategy)

    testExecutor.updateFinishedExecutionCounters(buildExecutionForJob(fooJob), "success")
    testExecutor.updateFinishedExecutionCounters(buildExecutionForJob(fooJob), "success")
    testExecutor.updateFinishedExecutionCounters(buildExecutionForJob(untaggedJob), "success")
    testExecutor.updateFinishedExecutionCounters(buildExecutionForJob(fooBarJob), "success")
    testExecutor.updateFinishedExecutionCounters(buildExecutionForJob(untaggedJob), "failure")
    testExecutor.updateFinishedExecutionCounters(buildExecutionForJob(fooBarJob), "failure")

    val metrics = Prometheus.serialize(
      testExecutor.getMetrics(Set(fooJob))(
        getStateAtomic = _ => {
          ((5, 1), 2)
        },
        runningExecutions = Seq(
          buildExecutionForJob(fooJob) -> ExecutionStatus.ExecutionRunning,
          buildExecutionForJob(fooJob) -> ExecutionStatus.ExecutionRunning,
          buildExecutionForJob(fooJob) -> ExecutionStatus.ExecutionWaiting,
          buildExecutionForJob(fooBarJob) -> ExecutionStatus.ExecutionRunning,
          buildExecutionForJob(untaggedJob) -> ExecutionStatus.ExecutionRunning,
          buildExecutionForJob(untaggedJob) -> ExecutionStatus.ExecutionWaiting
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
         |cuttle_scheduler_stat_count {type="failing"} 2
         |# HELP cuttle_scheduler_stat_count_by_tag The number of executions that we have in concrete states by tag
         |# TYPE cuttle_scheduler_stat_count_by_tag gauge
         |cuttle_scheduler_stat_count_by_tag {tag="foo", type="waiting"} 1
         |cuttle_scheduler_stat_count_by_tag {tag="foo", type="running"} 3
         |cuttle_scheduler_stat_count_by_tag {tag="bar", type="failing"} 2
         |cuttle_scheduler_stat_count_by_tag {tag="foo", type="failing"} 2
         |cuttle_scheduler_stat_count_by_tag {tag="bar", type="running"} 1
         |# HELP cuttle_scheduler_stat_count_by_job The number of executions that we have in concrete states by job
         |# TYPE cuttle_scheduler_stat_count_by_job gauge
         |cuttle_scheduler_stat_count_by_job {job="untagged_job", type="waiting"} 1
         |cuttle_scheduler_stat_count_by_job {job="foo_bar_job", type="running"} 1
         |cuttle_scheduler_stat_count_by_job {job="untagged_job", type="running"} 1
         |cuttle_scheduler_stat_count_by_job {job="foo_job", type="waiting"} 1
         |cuttle_scheduler_stat_count_by_job {job="untagged_job", type="failing"} 1
         |cuttle_scheduler_stat_count_by_job {job="foo_job", type="running"} 2
         |cuttle_scheduler_stat_count_by_job {job="foo_bar_job", type="failing"} 2
         |# HELP cuttle_executions_total The number of finished executions that we have in concrete states by job and by tag
         |# TYPE cuttle_executions_total counter
         |cuttle_executions_total {job_id="foo_job", tags="foo", type="failure"} 0
         |cuttle_executions_total {job_id="foo_bar_job", tags="foo,bar", type="success"} 1
         |cuttle_executions_total {job_id="foo_job", tags="foo", type="success"} 2
         |cuttle_executions_total {job_id="foo_bar_job", tags="foo,bar", type="failure"} 1
         |cuttle_executions_total {job_id="untagged_job", type="success"} 1
         |cuttle_executions_total {job_id="untagged_job", type="failure"} 1
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
      "project_name",
      "test_version"
    )

  private val fooTag = Tag("foo")
  private val barTag = Tag("bar")

  private val fooJob: Job[TestScheduling] = buildJob("foo_job", Set(fooTag))

  private val fooBarJob: Job[TestScheduling] = buildJob("foo_bar_job", Set(fooTag, barTag))

  private val untaggedJob: Job[TestScheduling] = buildJob("untagged_job")
}
