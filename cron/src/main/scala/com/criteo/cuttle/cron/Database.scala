package com.criteo.cuttle.cron

import java.time.Instant

import doobie.util.fragment.Fragment
import doobie.implicits._

private[cron] object Database {
  def sqlGetContextsBetween(start: Instant, end: Instant, job: CronJob): Fragment =
    sql"""
      SELECT context_id as id, context_id as json FROM executions
      WHERE
        executions.job = ${job.id}
        AND (start_time >= ${start} and start_time <= ${end}) or (end_time >= ${start} and end_time <= ${end})
    """
}
