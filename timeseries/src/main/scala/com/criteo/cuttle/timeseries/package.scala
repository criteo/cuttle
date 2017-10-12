package com.criteo.cuttle

import java.time.{Instant, ZoneId}

import scala.language.experimental.macros
import codes.reactive.scalatime._

import scala.reflect.macros.blackbox

/** A [[TimeSeries]] scheduler executes the [[com.criteo.cuttle.Workflow Workflow]] for the
  * time partitions defined in a calendar. Each [[com.criteo.cuttle.Job Job]] defines how it mnaps
  * to the calendar (for example Hourly or Daily UTC), and the [[com.criteo.cuttle.Scheduler Scheduler]]
  * ensure that at least one [[com.criteo.cuttle.Execution Execution]] is created and successfully run
  * for each defined Job/Period.
  *
  * The scheduler also allow to [[Backfill]] already computed partitions. The [[Backfill]] can be recursive
  * or not and an audit log of backfills is kept.
  */
package object timeseries {

  import TimeSeriesCalendar._

  /** Utility that allow to define compile time safe date literals. Meaning that compilation
    * will fail if the date literal cannot be parsed into a UTC instant.
    *
    * {{{
    * val start = date"2017-09-01T00:00:00Z"
    * }}}
    */
  implicit class SafeLiteralDate(val sc: StringContext) extends AnyVal {
    def date(args: Any*): Instant = macro safeLiteralDate
  }

  def safeLiteralDate(c: blackbox.Context)(args: c.Expr[Any]*): c.Expr[Instant] = {
    import c.universe._
    c.prefix.tree match {
      case Apply(_, List(Apply(_, Literal(Constant(dateString: String)) :: Nil))) =>
        scala.util.Try(Instant.parse(dateString)) match {
          case scala.util.Success(_) =>
            c.Expr(q"""java.time.Instant.parse($dateString)""")
          case scala.util.Failure(_) =>
            c.abort(c.enclosingPosition, s"Invalid date literal `$dateString'")
        }
      case _ =>
        c.abort(c.enclosingPosition, s"Only a single String literal is allowed here")
    }
  }

  /** Defines an implicit default dependency descriptor for [[TimeSeries]] graphs.
    * The default is `offset = 0`. */
  implicit val defaultDependencyDescriptor: TimeSeriesDependency =
    TimeSeriesDependency(0.hours)

  /** Defines an hourly calendar starting at the specified instant. Hours are defined as
    * complete calendar hours starting at 00 minutes, 00 seconds.
    *
    * If the start instant does not match a round hour (0 minutes, 0 seconds), the calendar
    * will actually start a the next hour immediatly following the start instant.
    *
    * @param start The instant this calendar will start.
    */
  def hourly(start: Instant) = TimeSeries(calendar = Hourly, start)

  /** Defines an daily calendar starting at the specified instant, and using the specified time zone.
    * Days are defined as complete calendar days starting a midnight and during 24 hours. If the specified
    * timezone defines lightsaving it is possible that some days are 23 or 25 horus thus.
    *
    * If the start instant does not match a round day (midnight), the calendar
    * will actually start a the next day immediatly following the start instant.
    *
    * @param start The instant this calendar will start.
    * @param tz The time zone for which these _days_ are defined.
    */
  def daily(start: Instant, tz: ZoneId) = TimeSeries(calendar = Daily(tz), start)

  /** Defines a monthly calendar. Months are defined as complete calendar months starting on the 1st day and
    * during 28,29,30 or 31 days. The specified time zone is used to define the exact month start instant.
    *
    * If the start instant does not match a round month (1st at midnight), the calendar
    * will actually start a the next month immediatly following the start instant.
    *
    * @param start The instant this calendar will start.
    * @param tz The time zone for which these months are defined.
    */
  def monthly(start: Instant, tz: ZoneId) = TimeSeries(calendar = Monthly(tz), start)

}
