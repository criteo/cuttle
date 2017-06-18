package com.criteo.cuttle.platforms.http

import com.criteo.cuttle._
import platforms.{ExecutionPool, RateLimiter}

import scala.concurrent._
import java.util.concurrent.{TimeUnit}

import lol.http._

case class HttpPlatform(maxConcurrentRequests: Int, rateLimits: Seq[(String, HttpPlatform.RateLimit)])
    extends ExecutionPlatform {

  private[HttpPlatform] val pool = new ExecutionPool(name = "http", concurrencyLimit = maxConcurrentRequests)
  private[HttpPlatform] val rateLimiters = rateLimits.map {
    case (pattern, HttpPlatform.RateLimit(tokens, per)) =>
      (pattern -> new RateLimiter(
        s"http.${pattern}",
        tokens,
        per match {
          case TimeUnit.DAYS => (24 * 60 * 60 * 1000) / tokens
          case TimeUnit.HOURS => (60 * 60 * 1000) / tokens
          case TimeUnit.MINUTES => (60 * 1000) / tokens
          case TimeUnit.SECONDS => (1000) / tokens
          case x => sys.error(s"Non supported period, ${x}")
        }
      ))
  }

  override def waiting: Set[Execution[_]] =
    rateLimiters.map(_._2).foldLeft(pool.waiting)(_ ++ _.waiting)

  override lazy val routes: PartialService = pool.routes
}

object HttpPlatform {
  case class RateLimit(maxRequests: Int, per: TimeUnit)

  def request[A, S <: Scheduling](request: Request)(thunk: Response => Future[A])(
    implicit execution: Execution[S]): Future[A] = {
    val streams = execution.streams
    streams.debug(s"HTTP request: ${request}")

    val httpPlatform =
      ExecutionPlatform.lookup[HttpPlatform].getOrElse(sys.error("No http execution platform configured"))
    httpPlatform.pool.run(execution) { () =>
      try {
        val host =
          request.headers.getOrElse(h"Host", sys.error("`Host' header must be present in the request")).toString
        val rateLimiter = httpPlatform.rateLimiters
          .collectFirst {
            case (pattern, rateLimiter) if host.matches(pattern) =>
              rateLimiter
          }
          .getOrElse(sys.error(s"A rate limiter should be defined for `${host}'"))

        rateLimiter.run(execution) { () =>
          Client.run(request) { response =>
            streams.debug(s"Got response: ${response}")
            thunk(response)
          }
        }
      } catch {
        case e: Throwable =>
          Future.failed(e)
      }
    }
  }

}
