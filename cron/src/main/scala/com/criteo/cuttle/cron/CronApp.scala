package com.criteo.cuttle.cron

import java.time.Instant
import java.util.concurrent.TimeUnit

import cats.data.EitherT
import cats.effect.IO
import com.criteo.cuttle.Auth._
import com.criteo.cuttle.Metrics.{Gauge, Prometheus}
import com.criteo.cuttle._
import com.criteo.cuttle.utils.getJVMUptime
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

    // we only show 20 recent executions by default but it could be modified via query parameter
    case GET at url"/api/cron/executions?dag=$dag&start=$start&end=$end&limit=$limit" =>
      val jsonOrError: EitherT[IO, Throwable, Json] = for {
        dag <- EitherT.fromOption[IO](workload.dags.find(_.id == dag), throw new Exception(s"Unknown job DAG $dag"))
        jobIds <- EitherT.rightT[IO, Throwable](dag.cronPipeline.vertices.map(_.id))
        startDate <- EitherT.rightT[IO, Throwable](Try(Instant.parse(start)).getOrElse(minStartDateForExecutions))
        endDate <- EitherT.rightT[IO, Throwable](Try(Instant.parse(end)).getOrElse(maxStartDateForExecutions))
        limit <- EitherT.rightT[IO, Throwable](Try(limit.toInt).getOrElse(Int.MaxValue))
        executions <- EitherT.right[Throwable](buildExecutionsList(executor, jobIds, startDate, endDate, limit))
        executionListFlat <- EitherT.rightT[IO, Throwable](executions.values.toSet.flatten)
        json <- EitherT.rightT[IO, Throwable](
          Json.fromValues(executionListFlat.map(ExecutionLog.executionLogEncoder.apply(_)))
        )
      } yield json

      jsonOrError.value.map {
        case Right(json) => Ok(json)
        case Left(e)     => BadRequest(Json.obj("error" -> Json.fromString(e.getMessage)))
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

    case POST at url"/api/dags/$id/pause" => { implicit user =>
      workload.dags.find(_.id == id).fold(NotFound) { dag =>
        scheduler.pauseDags(Set(dag), executor)
        Ok
      }
    }

    case POST at url"/api/dags/$id/runnow" => { implicit user =>
      workload.dags.find(_.id == id).fold(NotFound) { dag =>
        scheduler.runJobsNow(Set(dag), executor)
        Ok
      }
    }

    case POST at url"/api/dags/$id/runnow_redirect" => { implicit user =>
      workload.dags.find(_.id == id).fold(NotFound) { dag =>
        scheduler.runJobsNow(Set(dag), executor)
        Redirect("/", 302)
      }
    }

    case POST at url"/api/dags/$id/resume" => { implicit user =>
      workload.dags.find(_.id == id).fold(NotFound) { dag =>
        scheduler.resumeDags(Set(dag), executor)
        Ok
      }
    }

    case POST at url"/api/dags/$id/pause_redirect" => { implicit user =>
      workload.dags.find(_.id == id).fold(NotFound) { dag =>
        scheduler.pauseDags(Set(dag), executor)
        Redirect("/", 302)
      }
    }

    case POST at url"/api/dags/$id/resume_redirect" => { implicit user =>
      workload.dags.find(_.id == id).fold(NotFound) { dag =>
        scheduler.resumeDags(Set(dag), executor)
        Redirect("/", 302)
      }
    }
  }

  private val api = publicApi orElse project.authenticator(privateApi)
  private val uiRoutes = UI(project, executor).routes()

  val routes: PartialService = api
    .orElse(uiRoutes)
    .orElse {
      executor.platforms.foldLeft(PartialFunction.empty: PartialService) {
        case (s, p) => s.orElse(p.publicRoutes).orElse(project.authenticator(p.privateRoutes))
      }
    }
    .orElse {
      case _ => NotFound
    }
}
