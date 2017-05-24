package com.criteo.cuttle

import java.time.{Instant, ZoneId}

import scala.language.experimental.macros
import codes.reactive.scalatime._

import scala.reflect.macros.blackbox

package object timeseries {

  implicit class SafeLiteralDate(val sc: StringContext) extends AnyVal {
    def date(args: Any*): Instant = macro safeLiteralDate
  }

  def safeLiteralDate(c: blackbox.Context)(args: c.Expr[Any]*): c.Expr[Instant] = {
    import c.universe._
    c.prefix.tree match {
      case Apply(_, List(Apply(_, Literal(Constant(dateString: String)) :: Nil))) =>
        scala.util.Try(Instant.parse(dateString)) match {
          case scala.util.Success(_) =>
            c.Expr(
              q"""java.time.Instant.parse($dateString)""")
          case scala.util.Failure(_) =>
            c.abort(c.enclosingPosition, s"Invalid date literal `$dateString'")
        }
      case _ =>
        c.abort(c.enclosingPosition, s"Only a single String literal is allowed here")
    }
  }

  implicit val defaultDependencyDescriptor: TimeSeriesDependency =
    TimeSeriesDependency(0.hours)

  def hourly(start: Instant) = TimeSeriesScheduling(grid = Hourly, start)
  def daily(start: Instant, tz: ZoneId) = TimeSeriesScheduling(grid = Daily(tz), start)

}
