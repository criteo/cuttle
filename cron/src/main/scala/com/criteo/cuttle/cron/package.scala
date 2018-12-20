package com.criteo.cuttle

package object cron {
  type CronJob = Job[CronScheduling]
  type CronExecution = Execution[CronScheduling]
}
