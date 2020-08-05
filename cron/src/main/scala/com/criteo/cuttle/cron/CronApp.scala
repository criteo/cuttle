package com.criteo.cuttle.cron

import java.util.concurrent.TimeUnit

import cats.effect.IO
import cats.syntax.either._
import com.criteo.cuttle.Auth._
import com.criteo.cuttle.Metrics.{Gauge, Prometheus}
import com.criteo.cuttle._
import com.criteo.cuttle.utils.{getJVMUptime, sse}
import io.circe._
import io.circe.syntax._
import lol.http._
import lol.json._

import scala.util.{Success, Try}

private[cron] case class CronApp(project: CronProject, executor: Executor[CronScheduling])(
  implicit val transactor: XA
) {
  private val scheduler = project.scheduler
  private val workload: CronWorkload = project.workload

  private val allJobIds = workload.all.map(_.id)
  private val allDagIds = workload.dags.map(_.id)

  private def getDagsOrNotFound(json: Json): Either[Response, Set[CronDag]] =
    json.hcursor.downField("dags").as[Set[String]] match {
      case Left(_) => Left(BadRequest(s"Error: Cannot parse request body: $json"))
      case Right(dagIds) =>
        if (dagIds.isEmpty) Right(workload.dags)
        else {
          val filterDags = workload.dags.filter(v => dagIds.contains(v.id))
          if (filterDags.isEmpty) Left(NotFound)
          else Right(filterDags)
        }
    }

  private def getDagIdsFromJobIds(jobIds: Set[String]): Set[String] =
    for {
      dag <- workload.dags
      if dag.cronPipeline.vertices.exists(job => jobIds.contains(job.id))
    } yield dag.id

  val publicApi: PartialService = {
    case GET at "/version" => Ok(project.version)

    case GET at "/metrics" =>
      val metrics =
        executor.getMetrics(allJobIds, workload) ++
          scheduler.getMetrics(allJobIds, workload) :+
          Gauge("cuttle_jvm_uptime_seconds").labeled(("version", project.version), getJVMUptime)

      Ok(Prometheus.serialize(metrics))

    case GET at "/api/status" =>
      val projectJson = (status: String) =>
        Json.obj(
          "project" -> project.name.asJson,
          "version" -> Option(project.version).filterNot(_.isEmpty).asJson,
          "status" -> status.asJson
        )
      executor.healthCheck() match {
        case Success(_) => Ok(projectJson("ok"))
        case _          => InternalServerError(projectJson("ko"))
      }

    case GET at "/api/project_definition" => Ok(project.asJson)

    case GET at "/api/jobs_definition" => Ok(workload.asJson)

    case req @ GET at url"/api/executions/$id/streams" =>
      lazy val streams = executor.openStreams(id)
      req.headers.get(h"Accept").contains(h"text/event-stream") match {
        case true =>
          val stream = fs2.Stream(ServerSentEvents.Event("BOS".asJson)) ++ streams
            .through(fs2.text.utf8Decode)
            .through(fs2.text.lines)
            .chunks
            .map(chunk => ServerSentEvents.Event(Json.fromValues(chunk.toArray.toIterable.map(_.asJson)))) ++
            fs2.Stream(ServerSentEvents.Event("EOS".asJson))
          Ok(stream)
        case false =>
          val bodyFromStream = Content(
            stream = streams,
            headers = Map(h"Content-Type" -> h"text/plain")
          )
          Ok(bodyFromStream)
      }

    case GET at "/api/dags/paused" =>
      Ok(scheduler.getPausedDags.asJson)

    case request @ POST at url"/api/dags/states" =>
      request
        .readAs[Json]
        .flatMap { json =>
          json.hcursor
            .downField("dags")
            .as[Set[String]]
            .fold(
              df => IO.pure(BadRequest(s"Error: Cannot parse request body: $df")),
              dagIds => {
                val ids = if (dagIds.isEmpty) allDagIds else dagIds
                Ok(scheduler.getStats(ids))
              }
            )
        }

    case request @ POST at url"/api/statistics" => {
      def getStats(jobIds: Set[String]): IO[Option[(Json, Json)]] =
        executor
          .getStats(jobIds)
          .map(stats => Try(stats -> scheduler.getStats(getDagIdsFromJobIds(jobIds))).toOption)

      def asJson(x: (Json, Json)) = x match {
        case (executorStats, schedulerStats) =>
          executorStats.deepMerge(Json.obj("scheduler" -> schedulerStats))
      }

      request
        .readAs[Json]
        .flatMap { json =>
          json.hcursor
            .downField("jobs")
            .as[Set[String]]
            .fold(
              df => IO.pure(BadRequest(s"Error: Cannot parse request body: $df")),
              jobIds => {
                val ids = if (jobIds.isEmpty) allJobIds else jobIds
                getStats(ids).map(
                  _.map(stat => Ok(asJson(stat))).getOrElse(InternalServerError)
                )
              }
            )
        }
    }

    case request @ POST at url"/api/executions/status/$kind" => {
      def getExecutions(
        q: ExecutionsQuery
      ): IO[Option[(Int, List[ExecutionLog])]] = kind match {
        case "started" =>
          IO(
            Some(
              executor.runningExecutionsSizeTotal(q.jobIds(allJobIds)) -> executor
                .runningExecutions(
                  q.jobIds(allJobIds),
                  q.sort.column,
                  q.sort.asc,
                  q.offset,
                  q.limit
                )
                .toList
            )
          )
        case "retrying" =>
          IO(
            Some(
              executor.failingExecutionsSize(q.jobIds(allJobIds)) -> executor
                .failingExecutions(
                  q.jobIds(allJobIds),
                  q.sort.column,
                  q.sort.asc,
                  q.offset,
                  q.limit
                )
                .toList
            )
          )
        case "finished" =>
          executor
            .archivedExecutionsSize(q.jobIds(allJobIds))
            .map(ids => Some(ids -> executor.allRunning.toList))
        case _ =>
          IO.pure(None)
      }

      def asJson(q: ExecutionsQuery, x: (Int, Seq[ExecutionLog])): IO[Json] =
        x match {
          case (total, executions) =>
            (kind match {
              case "finished" =>
                executor
                  .archivedExecutions(
                    scheduler.allContexts,
                    q.jobIds(allJobIds),
                    q.sort.column,
                    q.sort.asc,
                    q.offset,
                    q.limit
                  )
                  .map(execs => execs.asJson)
              case _ =>
                IO(executions.asJson)
            }).map(
              data =>
                Json.obj(
                  "total" -> total.asJson,
                  "offset" -> q.offset.asJson,
                  "limit" -> q.limit.asJson,
                  "sort" -> q.sort.asJson,
                  "data" -> data
                )
            )
        }

      request
        .readAs[Json]
        .flatMap { json =>
          json
            .as[ExecutionsQuery]
            .fold(
              df => IO.pure(BadRequest(s"Error: Cannot parse request body: $df")),
              query => {
                getExecutions(query)
                  .flatMap(
                    _.map(e => asJson(query, e).map(json => Ok(json)))
                      .getOrElse(NotFound)
                  )
              }
            )
        }
    }

    case GET at url"/api/executions/$id?events=$events" =>
      def getExecution =
        IO.suspend(executor.getExecution(scheduler.allContexts, id))

      events match {
        case "true" | "yes" =>
          sse(getExecution, (e: ExecutionLog) => IO(e.asJson))
        case _ =>
          getExecution.map(_.map(e => Ok(e.asJson)).getOrElse(NotFound))
      }
  }

  val privateApi: AuthenticatedService = {
    case req @ GET at url"/api/shutdown" => { implicit user =>
      import scala.concurrent.duration._

      req.queryStringParameters.get("gracePeriodSeconds") match {
        case Some(s) =>
          Try(s.toLong) match {
            case Success(s) if s > 0 =>
              executor.gracefulShutdown(Duration(s, TimeUnit.SECONDS))
              Ok
            case _ =>
              BadRequest("gracePeriodSeconds should be a positive integer")
          }
        case None =>
          req.queryStringParameters.get("hard") match {
            case Some(_) =>
              executor.hardShutdown()
              Ok
            case None =>
              BadRequest("Either gracePeriodSeconds or hard should be specified as query parameter")
          }
      }
    }
    case request @ POST at url"/api/executions/relaunch" => { implicit user =>
      request
        .readAs[Json]
        .map { json =>
          json.hcursor
            .downField("jobs")
            .as[Set[String]]
            .fold(
              df => BadRequest(s"Error: Cannot parse request body: $df"),
              jobIds => {
                val ids = if (jobIds.isEmpty) allJobIds else jobIds
                executor.relaunch(ids)
                Ok
              }
            )
        }
    }

    case request @ POST at url"/api/dags/pause" => { implicit user =>
      request
        .readAs[Json]
        .map { json =>
          getDagsOrNotFound(json) match {
            case Left(a) => a
            case Right(dagIds) =>
              scheduler.pauseDags(dagIds, executor)
              Ok
          }
        }
    }

    case request @ POST at url"/api/dags/resume" => { implicit user =>
      request
        .readAs[Json]
        .map { json =>
          getDagsOrNotFound(json) match {
            case Left(a) => a
            case Right(dagIds) =>
              scheduler.resumeDags(dagIds, executor)
              Ok
          }
        }
    }

    case request @ POST at url"/api/dags/runnow" => { implicit user =>
      request
        .readAs[Json]
        .map { json =>
          getDagsOrNotFound(json) match {
            case Left(a) => a
            case Right(dagIds) =>
              scheduler.resumeDags(dagIds, executor)
              Ok
          }
        }
    }
  }

  private val api = publicApi orElse project.authenticator(privateApi)

  private val publicAssets: PartialService = {
    case GET at url"/public/cron/$file" =>
      ClasspathResource(s"/public/cron/$file").fold(NotFound)(r => Ok(r))
  }

  private val index: AuthenticatedService = {
    case req if req.url.startsWith("/api/") =>
      _ => NotFound
    case _ =>
      _ => Ok(ClasspathResource(s"/public/cron/index.html"))
  }

  val routes: PartialService = api
    .orElse {
      executor.platforms.foldLeft(PartialFunction.empty: PartialService) {
        case (s, p) => s.orElse(p.publicRoutes).orElse(project.authenticator(p.privateRoutes))
      }
    }
    .orElse(publicAssets orElse project.authenticator(index))
}
