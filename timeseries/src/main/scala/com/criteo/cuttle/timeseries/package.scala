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
    * will actually start the next hour immediatly following the start instant.
    *
    * The optional end instant allows to specify a finite calendar that will stop on the
    * end instant if it is a round hour or at the start of the hour otherwise.
    *
    * @param start The instant this calendar will start.
    * @param end The optional instant this calendar will end.
    */
  def hourly(start: Instant, end: Option[Instant] = None) = TimeSeries(calendar = Hourly, start, end)

  /** Defines an daily calendar starting at the specified instant, and using the specified time zone.
    * Days are defined as complete calendar days starting a midnight and during 24 hours. If the specified
    * timezone defines lightsaving it is possible that some days are 23 or 25 horus thus.
    *
    * If the start instant does not match a round day (midnight), the calendar
    * will actually start the next day immediatly following the start instant.
    *
    * The optional end instant allows to specify a finite calendar that will stop on the
    * end instant if it is a round day or at the start of the day otherwise.
    *
    * @param start The instant this calendar will start.
    * @param end The optional instant this calendar will end.
    * @param tz The time zone for which these _days_ are defined.
    */
  def daily(tz: ZoneId, start: Instant, end: Option[Instant] = None) = TimeSeries(calendar = Daily(tz), start, end)

  /** Defines a weekly calendar. Weeks are defined as complete calendar weeks starting on a specific
    * day of the week at midnight and lasting 7 days. The specified time zone is used to define the exact
    * week start instant.
    *
    * The start instant is used to define the first day of the week for the weeks.
    *
    * If the start instant does not match a round week (midnight), the calendar
    * will actually start the next week immediately following the start instant.
    *
    * The optional end instant allows to specify a finite calendar that will stop on the
    * end instant if it is a round week or at the start of the week otherwise.
    *
    * @param start The instant this calendar will start.
    * @param end The optional instant this calendar will end.
    * @param tz The time zone for which these _weeks_ are defined.
    */
  def weekly(tz: ZoneId, start: Instant, end: Option[Instant] = None) =
    TimeSeries(calendar = Weekly(tz, start.atZone(tz).getDayOfWeek), start, end)

  /** Defines a monthly calendar. Months are defined as complete calendar months starting on the 1st day and
    * during 28,29,30 or 31 days. The specified time zone is used to define the exact month start instant.
    *
    * If the start instant does not match a round month (1st at midnight), the calendar
    * will actually start the next month immediatly following the start instant.
    *
    * The optional end instant allows to specify a finite calendar that will stop on the
    * end instant if it is a round month or at the start of the month otherwise.
    *
    * @param start The instant this calendar will start.
    * @param end The optional instant this calendar will end.
    * @param tz The time zone for which these months are defined.
    */
  def monthly(tz: ZoneId, start: Instant, end: Option[Instant] = None) = TimeSeries(calendar = Monthly(tz), start, end)

}
