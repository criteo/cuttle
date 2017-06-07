package com.criteo.cuttle.timeseries

import TimeSeriesUtils._

import com.criteo.cuttle._

import lol.http._
import lol.json._

import io.circe._
import io.circe.syntax._

import algebra.lattice.Bool._

import scala.math.Ordering.Implicits._

import continuum._
import continuum.bound._

import java.time.{Instant, ZoneId}
import java.time.temporal.ChronoUnit._
import java.util.UUID

trait TimeSeriesApp { self: TimeSeriesScheduler =>
  import App._

  implicit val intervalEncoder = new Encoder[Interval[Instant]] {
    override def apply(interval: Interval[Instant]) = {
      val Closed(start) = interval.lower.bound
      val Open(end) = interval.upper.bound
      Json.obj(
        "start" -> start.asJson,
        "end" -> end.asJson
      )
    }
  }

  override def routes(graph: Graph[TimeSeriesScheduling],
                      executor: Executor[TimeSeriesScheduling],
                      xa: XA): PartialService = {

    case request @ GET at url"/api/timeseries/focus?start=$start&end=$end" =>
      def watchState() = Some((state, executor.allFailing))
      def getFocusView(watchedValue: Any = ()) = {
        val startDate = Instant.parse(start)
        val endDate = Instant.parse(end)
        val (_done, _running, _) = state
        val stucks = executor.allFailing
        val periodInterval = Interval.closedOpen(startDate, endDate)
        val period = StateD(Map.empty, IntervalSet(periodInterval))
        val done = and(StateD(_done), period)
        val running = and(StateD(_running), period)
        val remaining = and(complement(or(done, running)), period)
        val executions = List((done, "done"), (running, "running"), (remaining, "remaining"))
          .flatMap {
            case (state, label) =>
              for {
                (job, is) <- state.defined.toList
                interval <- is
                context <- splitInterval(job, interval, false).toList
              } yield {
                val lbl =
                  if (label == "running"
                      && stucks.exists {
                        case (stuckJob, stuckCtx) =>
                          stuckCtx.toInterval.intersect(periodInterval).isDefined
                      })
                    "stuck"
                  else
                    label
                job.id -> ((context.toInterval.intersect(periodInterval).get, lbl))
              }
          }
        val jobTimelines = executions
          .groupBy(_._1)
          .mapValues(_.map(_._2).sortBy(_._1))
        val summary =
          (for {
            (_, (interval, label)) <- executions
            Closed(start) = interval.lower.bound
            Open(end) = interval.upper.bound
            duration = start.until(end, HOURS)
            hour <- (1L to duration).map(i => Interval.closedOpen(start.plus(i - 1, HOURS), start.plus(i, HOURS)))
          } yield (hour, label))
            .groupBy(_._1)
            .map {
              case (hour, xs) =>
                (xs.filter(_._2 == "done").length.toFloat / xs.length, xs.exists(_._2 == "stuck"))
            }
            .toList
            .sortBy(_._1)
        Json.obj(
          "summary" -> summary.asJson,
          "jobs" -> jobTimelines.asJson
        )
      }
      if (request.headers.get(h"Accept").exists(_ == h"text/event-stream"))
        sse(watchState, getFocusView)
      else
        Ok(getFocusView())

    case GET at url"/api/timeseries/calendar?events=$events" =>
      def watchState() = Some((state, executor.allFailing))
      def getCalendar(watchedValue: Any = ()) = {
        val (done, running, _) = state
        val stucks = executor.allFailing
        val goal = StateD(
          graph.vertices.map(job => (job -> IntervalSet(Interval.atLeast(job.scheduling.start)))).toMap)
        val domain = and(or(StateD(done), StateD(running)), goal).defined
        val days = (for {
          (_, is) <- domain.toList
          interval <- is.toList
          Closed(start) = interval.lower.bound
          Open(end) = interval.upper.bound
          startDate = start.atZone(ZoneId.of("UTC")).toLocalDate
          endDate = end.atZone(ZoneId.of("UTC")).toLocalDate
          duration = startDate.until(endDate, DAYS)
          day <- (0L to duration).toList.map(i => startDate.plus(i, DAYS))
        } yield day).toSet
        val calendar = days.map { d =>
          val start = d.atStartOfDay(ZoneId.of("UTC")).toInstant
          val day = Interval.closedOpen(start, start.plus(1, DAYS))
          val doneForDay =
            and(StateD(done), StateD(Map.empty, IntervalSet(day)))
          val goalForDay = and(goal, StateD(Map.empty, IntervalSet(day)))
          val isStuck = stucks.exists(_._2.toInterval.intersect(day).isDefined)

          def totalDuration(state: StateD) =
            (for {
              (_, is) <- state.defined.toList
              interval <- is
            } yield {
              val Closed(start) = interval.lower.bound
              val Open(end) = interval.upper.bound
              start.until(end, SECONDS)
            }).fold(0L)(_ + _)
          (d.toString, f"${totalDuration(doneForDay).toDouble / totalDuration(goalForDay)}%1.1f", isStuck)
        }
        calendar.toList
          .sortBy(_._1)
          .map {
            case (date, completion, isStuck) =>
              (Map(
                "date" -> date.asJson,
                "completion" -> completion.asJson
              ) ++ (if (isStuck) Map("stuck" -> true.asJson) else Map.empty)).asJson
          }
          .asJson
      }
      events match {
        case "true" | "yes" =>
          sse(watchState, getCalendar)
        case _ =>
          Ok(getCalendar())
      }

    case POST at url"/api/timeseries/backfill?job=$id&startDate=$start&endDate=$end&priority=$priority" =>
      val job = this.state._1.keySet.find(_.id == id).get
      val startDate = Instant.parse(start)
      val endDate = Instant.parse(end)
      val backfillId = UUID.randomUUID().toString
      backfillJob(backfillId, job, startDate, endDate, priority.toInt)
      Ok(
        Json.obj(
          "id" -> backfillId.asJson
        ))
  }
}
