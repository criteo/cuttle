package com.criteo.cuttle.cron

import java.time.temporal.ChronoUnit
import java.time.{Duration, Instant, ZoneId, ZoneOffset}

import cron4s.Cron
import cron4s.lib.javatime._
import io.circe.syntax._
import io.circe.{Encoder, Json}

import scala.concurrent.duration._

/**
  * Configure a cron expression
  * @param cronExpression Cron expression to be parsed by https://github.com/alonsodomin/cron4s.
  *                       See the link above for more details.
  */
case class CronExpression(cronExpression: String, tz: ZoneId = ZoneOffset.UTC) {

  // https://www.baeldung.com/cron-expressions
  // https://www.freeformatter.com/cron-expression-generator-quartz.html
  private val cronExpr = Cron.unsafeParse(cronExpression)

  private def toZonedDateTime(instant: Instant) =
    instant.atZone(tz)

  def nextEvent(): Option[ScheduledAt] = {
    val instant = Instant.now()
    cronExpr.next(toZonedDateTime(instant)).map { next =>
      // add 1 second as between doesn't include the end of the interval
      val delay = Duration.between(instant, next).get(ChronoUnit.SECONDS).seconds.plus(1.second)
      ScheduledAt(next.toInstant, delay)
    }
  }
}

object CronExpression {
  implicit val encodeUser: Encoder[CronExpression] = new Encoder[CronExpression] {
    override def apply(cronExpression: CronExpression) =
      Json.obj("expression" -> cronExpression.cronExpression.asJson, "tz" -> cronExpression.tz.getId.asJson)
  }
}
