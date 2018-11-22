package com.criteo.cuttle.cron

import java.time.temporal.ChronoUnit
import java.time.{Duration, Instant, ZoneOffset}

import com.criteo.cuttle.{Job, Scheduling, SchedulingContext, Workload}
import cron4s.{Cron, _}
import cron4s.lib.javatime._

import scala.concurrent.duration._

private[cron] case class ScheduledAt(instant: Instant, delay: FiniteDuration)

/** A [[CronContext]] is passed to [[com.criteo.cuttle.Execution executions]] initiated by
  * the [[CronScheduler]].
  */
private[cron] case class CronContext(instant: Instant)(retryNum: Int) extends SchedulingContext {
  val retry: Int = retryNum

  def compareTo(other: SchedulingContext): Int = other match {
    case CronContext(otherInstant) =>
      instant.compareTo(otherInstant)
  }
}

/** Configure a [[com.criteo.cuttle.Job job]] as a [[CronScheduling]] job.
  *
  * @param cronExpression Cron expression to be parsed by https://github.com/alonsodomin/cron4s.
  *                       See the link above for more details.
  */
case class CronScheduling(cronExpression: String) extends Scheduling {
  override type Context = CronContext
  // https://www.baeldung.com/cron-expressions
  // https://www.freeformatter.com/cron-expression-generator-quartz.html
  private val cronExpr = Cron.unsafeParse(cronExpression)

  private def toZonedDateTime(instant: Instant) =
    instant.atZone(ZoneOffset.UTC)

  def nextEvent(): Option[ScheduledAt] = {
    val instant = Instant.now()
    cronExpr.next(toZonedDateTime(instant)).map { next =>
      // add 1 second as between doesn't include the end of the interval
      val delay = Duration.between(instant, next).get(ChronoUnit.SECONDS).seconds.plus(1.second)
      ScheduledAt(next.toInstant, delay)
    }
  }
}

/**
  * Class regrouping jobs for scheduler. It doesn't imply any order.
  * @param jobs Jobs to schedule.
  */
case class CronWorkload(jobs: Set[Job[CronScheduling]]) extends Workload[CronScheduling] {
  override def all: Set[Job[CronScheduling]] = jobs
}
