package com.criteo.cuttle.cron

import java.time.Instant
import java.util.concurrent.TimeUnit

import scala.util.{Success, Try}

import cats._
import cats.implicits._
import cats.data.EitherT
import cats.effect._

import com.criteo.cuttle.Metrics.{Gauge, Prometheus}
import com.criteo.cuttle._
import com.criteo.cuttle.utils.{getJVMUptime, sse}
import io.circe._
import io.circe.syntax._
import org.http4s._
import org.http4s.dsl.io._
import org.http4s.circe._
import org.http4s.headers.`Content-Type`
import com.criteo.cuttle.Auth._
import com.criteo.cuttle.Metrics.{Gauge, Prometheus}
import com.criteo.cuttle._
import com.criteo.cuttle.utils.getJVMUptime

private[cron] case class CronApp(project: CronProject, executor: Executor[CronScheduling])(
  implicit val transactor: XA
) {
  private val scheduler = project.scheduler
  private val workload: CronWorkload = project.workload

  private val allJobIds = workload.all.map(_.id)
  private val allDagIds = workload.dags.map(_.id)

  private def getDagsOrNotFound(json: Json): Either[IO[Response[IO]], Set[CronDag]] =
    json.hcursor.downField("dags").as[Set[String]] match {
      case Left(_) => Left(BadRequest(s"Error: Cannot parse request body: $json"))
      case Right(dagIds) =>
        if (dagIds.isEmpty) Right(workload.dags)
        else {
          val filterDags = workload.dags.filter(v => dagIds.contains(v.id))
          if (filterDags.isEmpty) Left(NotFound())
          else Right(filterDags)
        }
    }

  private def getDagIdsFromJobIds(jobIds: Set[String]): Set[String] =
    for {
      dag <- workload.dags
      if dag.cronPipeline.vertices.exists(job => jobIds.contains(job.id))
    } yield dag.id

  val publicApi: HttpRoutes[IO] = HttpRoutes.of[IO] {
    case GET -> Root / "version" => Ok(project.version)

    case GET -> Root / "metrics" =>
      val metrics =
        executor.getMetrics(allJobIds, workload) ++
          scheduler.getMetrics(allJobIds, workload) :+
          Gauge("cuttle_jvm_uptime_seconds").labeled(("version", project.version), getJVMUptime)

      Ok(Prometheus.serialize(metrics))

    case GET -> Root / "api" / "status" =>
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

    case GET -> Root / "api" / "project_definition" => Ok(project.asJson)

    case GET -> Root / "api" / "jobs_definition" => Ok(workload.asJson)

    case req @ GET -> Root / "api" / "executions" / id / "streams" =>
      lazy val streams = executor.openStreams(id)
      // TODO fix
      req.headers.get(org.http4s.headers.Accept).contains(MediaType.`text/event-stream`) match {
        case true =>
          val stream = fs2.Stream(ServerSentEvent("BOS")) ++ streams
            .through(fs2.text.utf8Decode)
            .through(fs2.text.lines)
            .chunks
            .map(chunk => ServerSentEvent(Json.fromValues(chunk.toArray.toIterable.map(_.asJson)).noSpaces)) ++
            fs2.Stream(ServerSentEvent("EOS"))
          Ok(stream)
        case false =>
          Ok(streams, `Content-Type`(MediaType.text.plain))
      }
    case GET -> Root / "api" / "jobs" / "paused" =>
      Ok(scheduler.getPausedDags.asJson)

    case request @ POST -> Root / "api" / "dags" / "states" =>
      request
        .as[Json]
        .flatMap { json =>
          json.hcursor
            .downField("dags")
            .as[Set[String]]
            .fold(
              df => BadRequest(s"Error: Cannot parse request body: $df"),
              dagIds => {
                val ids = if (dagIds.isEmpty) allDagIds else dagIds
                Ok(scheduler.getStats(ids))
              }
            )
        }

    case request @ POST -> Root / "api" / "statistics" => {
      def getStats(jobIds: Set[String]): IO[Option[(Json, Json)]] =
        executor
          .getStats(jobIds)
          .map(stats => Try(stats -> scheduler.getStats(getDagIdsFromJobIds(jobIds))).toOption)

      def asJson(x: (Json, Json)) = x match {
        case (executorStats, schedulerStats) =>
          executorStats.deepMerge(Json.obj("scheduler" -> schedulerStats))
      }

      request
        .as[Json]
        .flatMap { json =>
          json.hcursor
            .downField("jobs")
            .as[Set[String]]
            .fold(
              df => BadRequest(s"Error: Cannot parse request body: $df"),
              jobIds => {
                val ids = if (jobIds.isEmpty) allJobIds else jobIds
                getStats(ids).flatMap(
                  _.map(stat => Ok(asJson(stat))).getOrElse(InternalServerError())
                )
              }
            )
        }
    }

    case request @ POST -> Root / "api" / "executions" / "status" / kind => {
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
        .as[Json]
        .flatMap { json =>
          json
            .as[ExecutionsQuery]
            .fold(
              df => BadRequest(s"Error: Cannot parse request body: $df"),
              query => {
                getExecutions(query)
                  .flatMap(
                    _.map(e => asJson(query, e).flatMap(json => Ok(json)))
                      .getOrElse(NotFound())
                  )
              }
            )
        }
    }

    case request @ GET -> Root / "api" / "executions" / id =>
      val events = request.multiParams.getOrElse("events", "")
      def getExecution =
        IO.suspend(executor.getExecution(scheduler.allContexts, id))

      events match {
        case "true" | "yes" =>
          sse(getExecution, (e: ExecutionLog) => IO(e.asJson))
        case _ =>
          getExecution.flatMap(_.map(e => Ok(e.asJson)).getOrElse(NotFound()))
      }
  }

  def Redirect(path: String): IO[Response[IO]] =
    Found().map(_.withHeaders(org.http4s.headers.Location(Uri.fromString(path).toOption.get)))

  val privateApi: AuthedRoutes[User, IO] = AuthedRoutes.of {
    case req @ GET -> Root / "api" / "shutdown" as user =>
      import scala.concurrent.duration._

      req.req.params.get("gracePeriodSeconds") match {
        case Some(s) =>
          Try(s.toLong) match {
            case Success(s) if s > 0 =>
              executor.gracefulShutdown(Duration(s, TimeUnit.SECONDS))(user)
              Ok()
            case _ =>
              BadRequest("gracePeriodSeconds should be a positive integer")
          }
        case None =>
          req.req.params.get("hard") match {
            case Some(_) =>
              executor.hardShutdown()
              Ok()
            case None =>
              BadRequest("Either gracePeriodSeconds or hard should be specified as query parameter")
          }
      }
    case request @ POST -> Root / "api" / "executions" / "relaunch" as user => {
      request.req
        .as[Json]
        .flatMap { json =>
          json.hcursor
            .downField("jobs")
            .as[Set[String]]
            .fold(
              df => BadRequest(s"Error: Cannot parse request body: $df"),
              jobIds => {
                val ids = if (jobIds.isEmpty) allJobIds else jobIds
                executor.relaunch(ids)(user)
                Ok()
              }
            )
        }
    }

    case request @ POST -> Root / "api" / "dags" / "pause" as user => {
      request.req
        .as[Json]
        .flatMap { json =>
          getDagsOrNotFound(json) match {
            case Left(a) => a
            case Right(dagIds) =>
              scheduler.pauseDags(dagIds, executor)(implicitly, user)
              Ok()
          }
        }
    }

    case request @ POST -> Root / "api" / "dags" / "resume" as user => {
      request.req
        .as[Json]
        .flatMap { json =>
          getDagsOrNotFound(json) match {
            case Left(a) => a
            case Right(dagIds) =>
              scheduler.resumeDags(dagIds, executor)(implicitly, user)
              Ok()
          }
        }
    }

    case request @ POST -> Root / "api" / "dags" / "runnow" as user => {
      request.req
        .as[Json]
        .flatMap { json =>
          getDagsOrNotFound(json) match {
            case Left(a) => a
            case Right(dagIds) =>
              scheduler.runJobsNow(dagIds, executor)(implicitly, user)
              Ok()
          }
        }
    }
  }

  private val publicAssets = HttpRoutes.of[IO] {
    case GET -> Root / "public" / file =>
      import ThreadPools.Implicits.serverContextShift

      StaticFile
        .fromResource[IO](s"/public/cron/$file", ThreadPools.blockingExecutionContext)
        .getOrElseF(NotFound())
  }

  private val index: AuthedRoutes[User, IO] = AuthedRoutes.of {
    case req if req.req.uri.toString.startsWith("/api/") =>
      NotFound()
    case _ =>
      import ThreadPools.Implicits.serverContextShift

      StaticFile
        .fromResource[IO](s"/public/cron/index.html", ThreadPools.blockingExecutionContext)
        .getOrElseF(NotFound())
  }

  val routes: HttpRoutes[IO] = publicApi <+>
    executor.platforms.toList.foldMapK(_.publicRoutes) <+>
    publicAssets <+>
    project.authenticator(privateApi <+> executor.platforms.toList.foldMapK(_.privateRoutes) <+> index)

}
