package com.criteo.cuttle.cron

import java.time.Instant
import java.util.concurrent.TimeUnit

import scala.util.{Success, Try}

import cats.data.EitherT
import cats.effect.IO
import io.circe._
import io.circe.syntax._
import lol.http._
import lol.json._

import com.criteo.cuttle.Auth._
import com.criteo.cuttle.Metrics.{Gauge, Prometheus}
import com.criteo.cuttle._
import com.criteo.cuttle.utils.getJVMUptime

private[cron] case class CronApp(project: CronProject, executor: Executor[CronScheduling])(
  implicit val transactor: XA) {
  private val scheduler = project.scheduler
  private val workload = project.workload

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

    case GET at "/api/dashboard" =>
      Ok(scheduler.getStats(allJobIds))

    case GET at "/api/jobs/paused" =>
      Ok(scheduler.getPausedJobs.asJson)

    // we only show 20 recent executions by default but it could be modified via query parameter
    case GET at url"/api/cron/executions?job=$job&start=$start&end=$end&limit=$limit" =>
      val jsonOrError: EitherT[IO, Throwable, Json] = for {
        job <- EitherT.fromOption[IO](workload.all.find(_.id == job), throw new Exception(s"Unknow job $job"))
        startDate <- EitherT.rightT[IO, Throwable](Try(Instant.parse(start)).getOrElse(minStartDateForExecutions))
        endDate <- EitherT.rightT[IO, Throwable](Try(Instant.parse(end)).getOrElse(maxStartDateForExecutions))
        limit <- EitherT.rightT[IO, Throwable](Try(limit.toInt).getOrElse(Int.MaxValue))
        executionList <- EitherT.right[Throwable](buildExecutionsList(executor, job, startDate, endDate, limit))
      } yield executionList.asJson

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

    case POST at url"/api/jobs/$id/pause" => { implicit user =>
      workload.all.find(_.id == id).fold(NotFound) { job =>
        scheduler.pauseJobs(Set(job), executor)
        Ok
      }
    }

    case POST at url"/api/jobs/$id/resume" => { implicit user =>
      workload.all.find(_.id == id).fold(NotFound) { job =>
        scheduler.resumeJobs(Set(job), executor)
        Ok
      }
    }

    case POST at url"/api/jobs/$id/pause_redirect" => { implicit user =>
      workload.all.find(_.id == id).fold(NotFound) { job =>
        scheduler.pauseJobs(Set(job), executor)
        Redirect("/", 302)
      }
    }

    case POST at url"/api/jobs/$id/resume_redirect" => { implicit user =>
      workload.all.find(_.id == id).fold(NotFound) { job =>
        scheduler.resumeJobs(Set(job), executor)
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
