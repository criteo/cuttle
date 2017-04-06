package org.criteo.langoustine

import java.time.{ LocalDateTime, ZonedDateTime }

import scala.language.experimental.macros
import scala.reflect.macros.blackbox.Context

import codes.reactive.scalatime._

package object timeseries {

  implicit class SafeLiteralDate(val sc: StringContext) extends AnyVal {
    def date(args: Any*): LocalDateTime = macro safeLiteralDate
  }

  def safeLiteralDate(c: Context)(args: c.Expr[Any]*): c.Expr[LocalDateTime] = {
    import c.universe._
    c.prefix.tree match {
      case Apply(_, List(Apply(_, Literal(Constant(dateString: String)):: Nil))) =>
        scala.util.Try(ZonedDateTime.parse(dateString)) match {
          case scala.util.Success(_) =>
            c.Expr(q"""java.time.ZonedDateTime.parse($dateString).withZoneSameInstant(java.time.ZoneId.of("UTC")).toLocalDateTime""")
          case scala.util.Failure(_) =>
            c.abort(c.enclosingPosition, s"Invalid date literal `$dateString'")
        }
      case _ =>
        c.abort(c.enclosingPosition, s"Only a single String literal is allowed here")
    }
  }

  implicit val defaultDependencyDescriptor: TimeSeriesDependency =
    TimeSeriesDependency(0.hours)

  def hourly(start: LocalDateTime) = TimeSeriesScheduling(grid = Hourly, start)

}
