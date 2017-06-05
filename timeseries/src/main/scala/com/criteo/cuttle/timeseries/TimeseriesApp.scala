package com.criteo.cuttle.timeseries

import TimeSeriesUtils._

import com.criteo.cuttle.JsonApi._
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

  override def routes(graph: Graph[TimeSeriesScheduling]): PartialService = {
    case GET at "/api/timeseries" =>
      val (intervals, running, backfills) = state
      def stateToJson(m: State): Json = Json.obj(
        m.toSeq
          .map {
            case (job, is) =>
              job.id -> Json.fromValues(is.map(_.asJson))
          }
          .sortBy(_._1): _*
      )
      Ok(
        Json.obj(
          "jobs" -> stateToJson(intervals),
          "running" -> stateToJson(running),
          "backfills" -> Json.fromValues(backfills.toSeq.sortBy(_.id).map { backfill =>
            Json.obj(
              "id" -> backfill.id.asJson,
              "start" -> backfill.start.asJson,
              "end" -> backfill.end.asJson,
              "jobs" -> Json.fromValues(backfill.jobs.map(_.id.asJson)),
              "priority" -> backfill.priority.asJson
            )
          })
        ))

    case GET at url"/api/timeseries/focus?start=$start&end=$end" =>
      val startDate = Instant.parse(start)
      val endDate = Instant.parse(end)
      val (_done, _running, _) = state
      val periodInterval = Interval.closedOpen(startDate, endDate)
      val period = StateD(Map.empty, IntervalSet(periodInterval))
      val done = and(StateD(_done), period)
      val running = and(StateD(_running), period)
      val remaining = and(complement(or(done, running)), period)
      val executions = List((done, "done"), (running, "running"), (remaining, "remaining"))
        .flatMap { case (state, label) =>
          for {
            (job, is) <- state.defined.toList
            interval <- is
            context <- splitInterval(job, interval, false).toList
          } yield (job.id -> ((context.toInterval.intersect(periodInterval).get, label)))
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
          hour <- (1L to duration).map(i =>
            Interval.closedOpen(start.plus(i-1, HOURS), start.plus(i, HOURS)))
          } yield (hour, label))
        .groupBy(_._1)
        .mapValues(xs => xs.filter(_._2 == "done").length.toFloat / xs.length)
        .toList
        .sortBy(_._1)
      Ok(Json.obj(
        "summary" -> summary.asJson,
        "jobs" -> jobTimelines.asJson
      ))

    case GET at "/api/timeseries/calendar" =>
      val (done, running, _) = state
      val goal = StateD(graph.vertices.map(job => (job -> IntervalSet(Interval.atLeast(job.scheduling.start)))).toMap)
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
        val doneForDay =
          and(StateD(done), StateD(Map.empty, IntervalSet(Interval.closedOpen(start, start.plus(1, DAYS)))))
        val goalForDay = and(goal, StateD(Map.empty, IntervalSet(Interval.closedOpen(start, start.plus(1, DAYS)))))

        def totalDuration(state: StateD) =
          (for {
            (_, is) <- state.defined.toList
            interval <- is
          } yield {
            val Closed(start) = interval.lower.bound
            val Open(end) = interval.upper.bound
            start.until(end, SECONDS)
          }).fold(0L)(_ + _)

        (d.toString, totalDuration(doneForDay), totalDuration(goalForDay))
      }
      Ok(calendar.asJson)

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
