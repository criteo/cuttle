package com.criteo.cuttle.timeseries

import TimeSeriesUtils._

import com.criteo.cuttle.JsonApi._
import com.criteo.cuttle._

import lol.http._
import lol.json._

import io.circe._
import io.circe.syntax._
import io.circe.generic.semiauto._

import algebra.lattice.Bool._

import scala.math.Ordering.Implicits._

import continuum._
import continuum.bound._

import java.time.LocalDateTime
import java.util.UUID

case class TimeSeriesExecutionLog(
  id: Option[String],
  jobId: String,
  runningPeriod: Option[(LocalDateTime, Option[LocalDateTime])],
  context: TimeSeriesContext,
  status: Option[ExecutionStatus]
)
object TimeSeriesExecutionLog {
  implicit val encoder: Encoder[TimeSeriesExecutionLog] = deriveEncoder

  def fromExecutionLog(exec: ExecutionLog)(implicit jobs: Set[TimeSeriesJob]) = {
    val context = exec.context.as[TimeSeriesContext].right.get
    TimeSeriesExecutionLog(Some(exec.id), exec.job, Some((exec.startTime, exec.endTime)), context, Some(exec.status))
  }
}

trait TimeSeriesApp { self: TimeSeriesScheduler =>

  implicit val intervalEncoder = new Encoder[Interval[LocalDateTime]] {
    override def apply(interval: Interval[LocalDateTime]) = {
      val Closed(start) = interval.lower.bound
      val Open(end) = interval.upper.bound
      Json.obj(
        "start" -> start.asJson,
        "end" -> end.asJson
      )
    }
  }

  override def routes(graph: Graph[TimeSeriesScheduling], executor: Executor[TimeSeriesScheduling]): PartialService = {
    case GET at "/api/timeseries" =>
      val (intervals, backfills) = state
      Ok(
        Json.obj(
          "jobs" -> Json.obj(
            intervals.toSeq
              .map {
                case (job, intervals) =>
                  job.id -> Json.fromValues(intervals.map(_.asJson))
              }
              .sortBy(_._1): _*
          ),
          "backfills" -> Json.fromValues(backfills.toSeq.sortBy(_.id).map { backfill =>
            Json.obj(
              "id" -> backfill.id.asJson,
              "start" -> backfill.start.asJson,
              "end" -> backfill.end.asJson,
              "jobs" -> Json.fromValues(backfill.jobs.map(_.id.asJson)),
              "priority" -> backfill.priority.asJson
            )
          })
        ))

    case GET at url"/api/timeseries/focus?startDate=$start&endDate=$end" =>
      implicit val jobs = graph.vertices
      val startDate = LocalDateTime.parse(start)
      val endDate = LocalDateTime.parse(end)
      val sqlQueryContexts = Database.sqlGetContextsBetween(Some(startDate), Some(endDate))
      val archived = executor.archivedExecutions(sqlQueryContexts, "startTime", false, 0, 1000).map(TimeSeriesExecutionLog.fromExecutionLog)
      val running = executor.rawRunningExecutions
      val paused = executor.rawPausedExecutions
      val launched = (running ++ paused).map(TimeSeriesExecutionLog.fromExecutionLog)
      val launchedDomain = (for {
        execLog <- launched
        is = IntervalSet(execLog.context.toInterval)
        job = jobs.find(_.id == execLog.jobId).get
      } yield StateD(Map(job -> is), IntervalSet.empty)).fold(zero[StateD])(or(_, _))
      val domainInterval = Interval.closedOpen(startDate, endDate)
      val toDo: Seq[TimeSeriesExecutionLog] = {
        val (st, _) = state
        val done = StateD(st, IntervalSet.empty)
        val jobDomain =
          StateD(jobs.map(job => job -> IntervalSet(Interval.atLeast(job.scheduling.start))).toMap, IntervalSet.empty)
        val domain = StateD(Map.empty, IntervalSet(domainInterval))
        for {
          (job, is) <- List(jobDomain, complement(done), complement(launchedDomain), domain)
            .reduce(and(_, _))
            .defined
            .toList
          interval <- is.toList
          context <- splitInterval(job, interval, false)
        } yield
          TimeSeriesExecutionLog(
            id = None,
            runningPeriod = None,
            context = context,
            jobId = job.id,
            status = None
          )
      }
      Ok((archived ++ launched ++ toDo).filter(execLog => domainInterval.intersects(execLog.context.toInterval)).asJson)

    case POST at url"/api/timeseries/backfill?job=$id&startDate=$start&endDate=$end&priority=$priority" =>
      val job = this.state._1.keySet.find(_.id == id).get
      val startDate = LocalDateTime.parse(start)
      val endDate = LocalDateTime.parse(end)
      val backfillId = UUID.randomUUID().toString
      backfillJob(backfillId, job, startDate, endDate, priority.toInt)
      Ok(
        Json.obj(
          "id" -> backfillId.asJson
        ))
  }
}
