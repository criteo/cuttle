package com.criteo.cuttle.cron

import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId}

import cats.Foldable
import cats.data.EitherT
import cats.effect.IO
import cats.implicits._
import com.criteo.cuttle.{ExecutionLog, ExecutionStatus, Executor, XA}
import io.circe.Json
import lol.html._
import lol.http._
import lol.json._

import scala.util.Try

private[cron] case class UI(project: CronProject, executor: Executor[CronScheduling])(implicit val transactor: XA) {
  private val scheduler = project.scheduler
  private val workload = project.workload

  private val allDagIds = workload.dags.map(_.id)

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

  private implicit val stateToHtml = ToHtml { state: Either[Instant, Set[CronExecution]] =>
    state.fold(
      instant => tmpl"Scheduled at @timeFormat(instant)",
      executions => foldHtml(executions.toList)(e => tmpl"<p>Running @e.job.id</p>")
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

  private implicit val pausedToHtml = ToHtml { pausedDags: Map[CronDag, PausedDag] =>
    foldHtml(pausedDags.toList.sortBy(_._1.id)) {
      case (cronDag, pausedDag) =>
        tmpl"""
          <tr style="background: @ROSE">
            <td>@cronDag.id</td>
            <td>@cronDag.name</td>
            <td>@cronDag.cronExpression.tz.getId @cronDag.cronExpression.cronExpression</td>
            <td>Paused by @pausedDag.user.userId at @timeFormat(pausedDag.date) </td>
            <td><a href="/dags/@cronDag.id/runs?limit=20">Runs</a></td>
            <td>
              <form method="POST" action="/api/dags/@cronDag.id/resume_redirect"/>
              <input type="submit" value="Resume">
              </form>
            </td>
            <td></td>
          </tr>
        """
    }
  }

  case class JobRun(timestamp: Instant, log: ExecutionLog, index: Integer, jobsInDag: Integer)

  private implicit def jobRunToHtml = ToHtml { jobRun: JobRun =>
    {
      if (jobRun.index == 0) {
        tmpl"""
            <tr>
            <th rowspan=@{Html(String.valueOf(jobRun.jobsInDag))}>@{jobRun.timestamp.toString}</th>
            <td>@jobRun.log.job</td>
            <td>@jobRun.log.startTime.fold("-")(d => timeFormat(d))</td>
            <td>@jobRun.log.endTime.fold("-")(d => timeFormat(d))</td>
            @jobRun.log.status
            <td><a href="/execution/@jobRun.log.id/streams">Streams</a></td>
            </tr>
             """
      } else {
        tmpl"""
            <tr>
            <td>@jobRun.log.job</td>
            <td>@jobRun.log.startTime.fold("-")(d => timeFormat(d))</td>
            <td>@jobRun.log.endTime.fold("-")(d => timeFormat(d))</td>
            @jobRun.log.status
            <td><a href="/execution/@jobRun.log.id/streams">Streams</a></td>
            </tr>
             """
      }
    }
  }

  def home(activeAndPausedDags: (Map[CronDag, Either[Instant, Set[CronExecution]]], Map[CronDag, PausedDag])) = {
    val (activeDags, pausedDags) = activeAndPausedDags
    Layout(
      tmpl"""
        <table border="1" width="100%">
          <thead>
          <tr>
            @th("ID")
            @th("Name")
            @th("Cron Expression")
            @th("State")
            @th("Runs")
            @th("Pause")
            @th("Run Now")
          </tr>
        </thead>
        @foldHtml(activeDags.toList.sortBy(_._1.id)) {
          case (cronDag, state) =>
            <tr>
              <td>@cronDag.id</td>
              <td>@cronDag.name</td>
              <td>@cronDag.cronExpression.tz.getId @cronDag.cronExpression.cronExpression</td>
              <td>@state</td>
              <td><a href="/dags/@cronDag.id/runs?limit=20">Runs</a></td>
              <td>
                <form method="POST" action="/api/dags/@cronDag.id/pause_redirect"/>
                  <input type="submit" value="Pause">
                </form>
              </td>
            <td>
              <form method="POST" action="/api/dags/@cronDag.id/runnow_redirect"/>
                <input type="submit" value="Run Now">
              </form>
            </td>
          </tr>
        }
        @pausedDags
      </table>
    """
    )
  }

  def runs(dag: CronDag, runs: Map[Instant, Seq[ExecutionLog]]) = {

    val dagName = if (!dag.name.isEmpty) dag.name else dag.id
    Layout(tmpl"""
      <h3>@dagName</h3>
      <table border="1" width="100%">
        <thead>
          <tr>
            @th("Run")
            @th("Job Id")
            @th("Start Date")
            @th("End Date")
            @th("Status")
            @th("Logs")
          </tr>
        </thead>
        @foldHtml(runs.toSeq.toList.sortBy(_._1.toEpochMilli).reverse){
          case (instant, logs) =>
              @foldHtml(logs.zipWithIndex.map(r => JobRun(instant, r._1, r._2, logs.length)).toList) {
                case (jobRun) => @jobRun
              }
        }
      </table>
      <a href="/dags/@dag.id/runs">Show all runs, it could take some time to render	☺</a>
    """)
  }

  def routes(): PartialService = {
    // we show all executions by default but it could be modified via query parameter and it usually set to 20.
    case GET at url"/dags/$dagId/runs?limit=$limit" =>
      val executionList = for {
        dag <- EitherT.fromOption[IO](workload.dags.find(_.id == dagId), throw new Exception(s"Unknown dag $dagId"))
        jobIds <- EitherT.rightT[IO, Throwable](dag.cronPipeline.vertices.map(_.id))
        limit <- EitherT.rightT[IO, Throwable](Try(limit.toInt).getOrElse(Int.MaxValue))
        executionList <- EitherT.right[Throwable](
          buildExecutionsList(executor, jobIds, None, None, limit)
        )
      } yield (dag, executionList)

      executionList.value.map {
        case Right((dag, executionList)) => Ok(runs(dag, executionList))
        case Left(e)                     => BadRequest(Json.obj("error" -> Json.fromString(e.getMessage)))
      }

    case GET at url"/execution/$executionId/streams" =>
      Redirect(s"/api/executions/$executionId/streams")

    case GET at "/" =>
      Ok(
        home(scheduler.snapshot(allDagIds))
      )
  }
}
