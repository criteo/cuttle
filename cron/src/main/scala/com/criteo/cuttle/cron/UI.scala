package com.criteo.cuttle.cron

import java.time.{Instant, ZoneId}
import java.time.format.DateTimeFormatter

import scala.util.Try

import cats.Foldable
import cats.data.EitherT
import cats.effect.IO
import cats.implicits._
import lol.http._
import lol.html._
import lol.json._
import io.circe.Json

import com.criteo.cuttle.{ExecutionLog, ExecutionStatus, Executor, PausedJob, XA}

private[cron] case class UI(project: CronProject, executor: Executor[CronScheduling])(implicit val transactor: XA) {
  private val scheduler = project.scheduler
  private val workload = project.workload

  private val allJobIds = workload.all.map(_.id)

  private val timeFormat =
    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss 'UTC'").withZone(ZoneId.of("UTC")).format _

  private val GREEN = "#62cd63"
  private val RED = "#ff6263"
  private val GRAY = "#d7e2ea"
  private val BLUE = "#26a69a"
  private val YELLOW = "#f09c3a"
  private val ROSE = "#ffaaff"

  import scala.language.higherKinds
  private def foldHtml[A, F[_]: Foldable](fa: F[A])(f: A => Html) = {
    val foldable = implicitly[Foldable[F]]
    foldable.foldLeft(fa, Html.empty) {
      case (html, a) =>
        tmpl"""
          @html
          @f(a)
        """
    }
  }

  private def th(text: String) =
    tmpl"""
      <th style="text-align: left; background: #f1f1f1; padding: 5px;" rowspan="2">
        @text
      </th>
     """

  private implicit val stateToHtml = ToHtml { state: Either[Instant, CronExecution] =>
    state.fold(
      instant => tmpl"Scheduled at @timeFormat(instant)",
      execution => tmpl"Running @execution.id, retry: @execution.context.retry"
    )
  }

  private object Layout {

    def apply(content: Html) = {
      val env = project.env._1
      tmpl"""
        <head>
          <title>@project.name</title>
          <link rel="icon" type="image/png" href="/public/favicon-256.png" />
          <style>
            body, input { font-family: serif; font-size: 16px; }
            code { color: #0f5db5; }
            .highlight em { background: #fff59a; }
          </style>
        </head>
        <body style="padding-bottom: 50px">
          <h2 style="margin:0; float: left; height: 40px;">
            <a href="/" style="text-decoration: none; color: black; background: #55FFFF; padding: 0 5px">
              @project.name
            </a>
            &nbsp;
            @project.version
            &nbsp;
            @env
          </h2>
          <hr style="clear: both; margin: 10px 0;">
          @content
        </body>
      """
    }
  }

  private implicit val pausedToHtml = ToHtml { pausedJobs: Map[CronJob, PausedJob] =>
    foldHtml(pausedJobs.toList) {
      case (cronJob, pausedJob) =>
        tmpl"""
          <tr style="background: @ROSE">
            <td>@cronJob.id</td>
            <td>@cronJob.name</td>
            <td>@cronJob.scheduling.cronExpression</td>
            <td>@cronJob.scheduling.maxRetry</td>
            <td>Paused by @pausedJob.user.userId at @timeFormat(pausedJob.date) </td>
            <td><a href="/job/@cronJob.id/executions?limit=20">Executions</a></td>
            <td>
              <form method="POST" action="/api/jobs/@cronJob.id/resume_redirect"/>
              <input type="submit" value="Resume">
              </form>
            </td
          </tr>
        """
    }
  }

  def home(activeAndPausedJobs: (Map[CronJob, Either[Instant, CronExecution]], Map[CronJob, PausedJob])) = {
    val (activeJobs, pausedJobs) = activeAndPausedJobs
    Layout(
      tmpl"""
        <table border="1" width="100%">
          <thead>
          <tr>
            @th("ID")
            @th("Name")
            @th("Cron Expression")
            @th("Max Retry")
            @th("State")
            @th("Executions")
            @th("Pause")
          </tr>
        </thead>
        @foldHtml(activeJobs.toList) {
          case (cronJob, state) =>
            <tr>
              <td>@cronJob.id</td>
              <td>@cronJob.name</td>
              <td>@cronJob.scheduling.cronExpression</td>
              <td>@cronJob.scheduling.maxRetry</td>
              <td>@state</td>
              <td><a href="/job/@cronJob.id/executions?limit=20">Executions</a></td>
              <td>
                <form method="POST" action="/api/jobs/@cronJob.id/pause_redirect"/>
                  <input type="submit" value="Pause">
                </form>
              </td
            </tr>
        }
        @pausedJobs
      </table>
    """
    )
  }

  private implicit val executionStatusToHtml = ToHtml[ExecutionStatus] {
    case ExecutionStatus.ExecutionSuccessful => tmpl"""<td style="background: @GREEN">Successful</td>"""
    case ExecutionStatus.ExecutionFailed     => tmpl"""<td style="background: @RED">Failed</td>"""
    case ExecutionStatus.ExecutionRunning    => tmpl"""<td style="background: @BLUE">Running</td>"""
    case ExecutionStatus.ExecutionTodo       => tmpl"""<td style="background: @GRAY">TODO</td>"""
    case ExecutionStatus.ExecutionWaiting    => tmpl"""<td style="background: @YELLOW">Waiting</td>"""
    case ExecutionStatus.ExecutionThrottled  => tmpl"""<td style="background: @YELLOW">Throttled</td>"""
    case ExecutionStatus.ExecutionPaused     => tmpl"""<td style="background: @ROSE">Paused</td>"""
    case _                                   => tmpl"""<td style="background: @GRAY">Unknown</td>"""
  }

  def executions(job: CronJob, executionLogs: Seq[ExecutionLog]) = {
    val jobName = if (!job.name.isEmpty) job.name else job.id
    Layout(tmpl"""
      <h3>@jobName</h3>
      <table border="1" width="100%">
        <thead>
          <tr>
            @th("ID")
            @th("Start Date")
            @th("End Date")
            @th("Status")
            @th("Logs")
          </tr>
        </thead>
        @foldHtml(executionLogs.toList) {
          el =>
            <tr>
              <td>@el.id</td>
              <td>@el.startTime.fold("-")(d => timeFormat(d))</td>
              <td>@el.endTime.fold("-")(d => timeFormat(d))</td>
              @el.status
              <td><a href="/execution/@el.id/streams">Streams</a></td>
            </tr>
        }
      </table>
      <a href="/job/@job.id/executions">Show all executions, it could take some to render	☺</a>
    """)
  }

  def routes(): PartialService = {
    // we show all executions by default but it could be modified via query parameter and it usually set to 20.
    case GET at url"/job/$jobId/executions?limit=$limit" =>
      val jobAndExecutionListOrError = for {
        job <- EitherT.fromOption[IO](workload.all.find(_.id == jobId), throw new Exception(s"Unknown job $jobId"))
        limit <- EitherT.rightT[IO, Throwable](Try(limit.toInt).getOrElse(Int.MaxValue))
        executionList <- EitherT.right[Throwable](
          buildExecutionsList(executor, job, minStartDateForExecutions, maxStartDateForExecutions, limit))
      } yield job -> executionList

      jobAndExecutionListOrError.value.map {
        case Right((job, executionLogs)) => Ok(executions(job, executionLogs))
        case Left(e)                     => BadRequest(Json.obj("error" -> Json.fromString(e.getMessage)))
      }

    case GET at url"/execution/$executionId/streams" =>
      Redirect(s"/api/executions/$executionId/streams")

    case GET at "/" =>
      Ok(
        home(scheduler.snapshot(allJobIds))
      )
  }
}
