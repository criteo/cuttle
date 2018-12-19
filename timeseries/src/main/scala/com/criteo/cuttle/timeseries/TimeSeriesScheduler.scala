package com.criteo.cuttle.timeseries

import java.time.ZoneOffset.UTC
import java.time._
import java.time.temporal.ChronoUnit._
import java.time.temporal.{ChronoUnit, TemporalAdjusters}
import java.util.{Comparator, UUID}

import scala.collection.mutable
import scala.concurrent._
import scala.concurrent.duration.{Duration => ScalaDuration}
import scala.concurrent.stm.Txn.ExternalDecider
import scala.concurrent.stm._
import scala.math.Ordering.Implicits._

import cats._
import cats.effect.IO
import cats.implicits._
import de.sciss.fingertree.RangedSeq
import doobie._
import doobie.implicits._
import io.circe._
import io.circe.generic.semiauto._
import io.circe.syntax._

import com.criteo.cuttle.ThreadPools.Implicits.sideEffectThreadPool
import com.criteo.cuttle.ThreadPools._
import com.criteo.cuttle.Metrics._
import com.criteo.cuttle._
import com.criteo.cuttle.timeseries.Internal._
import com.criteo.cuttle.timeseries.TimeSeriesCalendar.{Daily, Hourly, Monthly, Weekly}
import com.criteo.cuttle.timeseries.intervals.Bound.{Bottom, Finite, Top}
import com.criteo.cuttle.timeseries.intervals.{Interval, IntervalMap}

/** Represents calendar partitions for which a job will be run by the [[TimeSeriesScheduler]].
  * See the companion object for the available calendars. */
sealed trait TimeSeriesCalendar {
  private[timeseries] def next(t: Instant): Instant
  private[timeseries] def truncate(t: Instant): Instant
  private[timeseries] def ceil(t: Instant): Instant = {
    val truncated = truncate(t)
    if (truncated == t) t
    else next(t)
  }
  private[timeseries] def inInterval(interval: Interval[Instant], maxPeriods: Int) = {
    def go(lo: Instant, hi: Instant, acc: List[(Instant, Instant)]): List[(Instant, Instant)] = {
      val nextLo = next(lo)
      if (nextLo.isAfter(hi)) acc
      else go(nextLo, hi, (lo, nextLo) +: acc)
    }
    interval match {
      case Interval(Finite(lo), Finite(hi)) =>
        go(ceil(lo), hi, List.empty).reverse.grouped(maxPeriods).map(xs => (xs.head._1, xs.last._2))
      case _ =>
        sys.error("panic")
    }
  }
  private[timeseries] def split(interval: Interval[Instant]) = {
    def go(lo: Instant, hi: Instant, acc: List[(Instant, Instant)]): List[(Instant, Instant)] = {
      val nextLo = next(lo)
      if (nextLo.isBefore(hi)) go(nextLo, hi, (lo, nextLo) +: acc)
      else (lo, hi) +: acc
    }
    interval match {
      case Interval(Finite(lo), Finite(hi)) =>
        go(lo, hi, List.empty).reverse
      case _ =>
        sys.error("panic")
    }
  }
}

private[timeseries] sealed trait TimeSeriesCalendarView {
  def calendar: TimeSeriesCalendar
  def upper(): TimeSeriesCalendarView
  val aggregationFactor: Int
}

/** Define the available calendars. */
object TimeSeriesCalendar {

  /** An hourly calendar. Hours are defined as complete calendar hours starting
    * at 00 minutes, 00 seconds. */
  case object Hourly extends TimeSeriesCalendar {
    def truncate(t: Instant) = t.truncatedTo(HOURS)
    def next(t: Instant) =
      t.truncatedTo(HOURS).plus(1, HOURS)
  }

  /** An daily calendar. Days are defined as complete calendar days starting a midnight and
    * during 24 hours. If the specified timezone defines lightsaving it is possible that some
    * days are 23 or 25 horus thus.
    *
    * @param tz The time zone for which these _days_ are defined.
    */
  case class Daily(tz: ZoneId) extends TimeSeriesCalendar {
    def truncate(t: Instant) = t.atZone(tz).truncatedTo(DAYS).toInstant
    def next(t: Instant) = t.atZone(tz).truncatedTo(DAYS).plus(1, DAYS).toInstant
  }

  /** A weekly calendar. Weeks are defined as complete calendar weeks starting on a specified day of week
    * at midnight and lasting 7 days. The specified time zone is used to define the exact week start instant.
    *
    * @param tz The time zone for which these _weeks_ are defined.
    * @param firstDay The first day of the week for these weeks.
    */
  case class Weekly(tz: ZoneId, firstDay: DayOfWeek) extends TimeSeriesCalendar {
    private def truncateToWeek(t: ZonedDateTime) =
      t.`with`(TemporalAdjusters.previousOrSame(firstDay)).truncatedTo(DAYS)
    def truncate(t: Instant) = truncateToWeek(t.atZone(tz)).toInstant
    def next(t: Instant) = truncateToWeek(t.atZone(tz)).plus(1, WEEKS).toInstant
  }

  /** An monthly calendar. Months are defined as complete calendar months starting on the 1st day and
    * during 28,29,30 or 31 days. The specified time zone is used to define the exact month start instant.
    *
    * @param tz The time zone for which these _months_ are defined.
    */
  case class Monthly(tz: ZoneId) extends TimeSeriesCalendar {
    private val truncateToMonth = (t: ZonedDateTime) =>
      t.`with`(TemporalAdjusters.firstDayOfMonth()).truncatedTo(ChronoUnit.DAYS)
    def truncate(t: Instant) = truncateToMonth(t.atZone(tz)).toInstant
    def next(t: Instant) = truncateToMonth(t.atZone(tz)).plus(1, MONTHS).toInstant
  }

  private[timeseries] implicit val calendarEncoder = new Encoder[TimeSeriesCalendar] {
    override def apply(calendar: TimeSeriesCalendar) = calendar match {
      case Hourly => Json.obj("period" -> "hourly".asJson)
      case Daily(tz: ZoneId) =>
        Json.obj(
          "period" -> "daily".asJson,
          "zoneId" -> tz.getId().asJson
        )
      case Weekly(tz: ZoneId, firstDay: DayOfWeek) =>
        Json.obj(
          "period" -> "weekly".asJson,
          "zoneId" -> tz.getId.asJson,
          "firstDay" -> firstDay.toString.asJson
        )
      case Monthly(tz: ZoneId) =>
        Json.obj(
          "period" -> "monthly".asJson,
          "zoneId" -> tz.getId().asJson
        )
    }
  }
}

private[timeseries] object TimeSeriesCalendarView {
  def apply(calendar: TimeSeriesCalendar) = calendar match {
    case TimeSeriesCalendar.Hourly               => new HourlyView(1)
    case TimeSeriesCalendar.Daily(tz)            => new DailyView(tz, 1)
    case TimeSeriesCalendar.Weekly(tz, firstDay) => new WeeklyView(tz, firstDay, 1)
    case TimeSeriesCalendar.Monthly(tz)          => new MonthlyView(tz, 1)
  }
  sealed trait GenericView extends TimeSeriesCalendarView {
    def over: (Int, TimeSeriesCalendar)
    def calendar = over._2
    def truncate(t: Instant) = calendar.truncate(t)
    def next(t: Instant) = (1 to over._1).foldLeft(calendar.truncate(t))((acc, _) => calendar.next(acc))
    def upper(): TimeSeriesCalendarView
  }
  case class HourlyView(aggregationFactor: Int) extends GenericView {
    def over = (1, Hourly)
    override def upper: TimeSeriesCalendarView = new DailyView(UTC, aggregationFactor * 24)
  }
  case class DailyView(tz: ZoneId, aggregationFactor: Int) extends GenericView {
    def over = (1, Daily(tz))
    override def upper: TimeSeriesCalendarView = new WeeklyView(tz, DayOfWeek.MONDAY, aggregationFactor * 7)
  }
  case class WeeklyView(tz: ZoneId, firstDay: DayOfWeek, aggregationFactor: Int) extends GenericView {
    def over = (1, Weekly(tz, firstDay))
    override def upper: TimeSeriesCalendarView = new MonthlyView(tz, aggregationFactor * 4)
  }
  case class MonthlyView(tz: ZoneId, aggregationFactor: Int) extends GenericView {
    def over = (1, Monthly(tz))
    override def upper: TimeSeriesCalendarView = new MonthlyView(tz, 1)
  }
}

/** A [[Backfill]] allows to recompute already computed time partitions in the past.
  *
  * @param id Unique id for the backfill.
  * @param start Start instant for the partitions to backfill.
  * @param end End instant for the partitions to backfill.
  * @param jobs Indicates the part of the graph to backfill.
  * @param priority The backfill priority. If minus than 0 it is less priority than the day
  *                 to day workload. If more than 0 it becomes more prioritary and can delay
  *                 the day to day workload.
  * @param description Description (for audit logs).
  * @param status Status of the backfill.
  * @param createdBy User who created the backfill (for audit logs).
  */
case class Backfill(id: String,
                    start: Instant,
                    end: Instant,
                    jobs: Set[Job[TimeSeries]],
                    priority: Int,
                    name: String,
                    description: String,
                    status: String,
                    createdBy: String)

private[timeseries] object Backfill {
  implicit val eqInstance: Eq[Backfill] = Eq.fromUniversalEquals[Backfill]
  implicit val encoder: Encoder[Backfill] = deriveEncoder
  implicit def decoder(implicit jobs: Set[Job[TimeSeries]]) =
    deriveDecoder[Backfill]
}

/** A [[TimeSeriesContext]] is passed to [[com.criteo.cuttle.Execution Executions]] initiated by
  * the [[TimeSeriesScheduler]].
  *
  * @param start Start instant of the partition to compute.
  * @param end End instant of the partition to compute.
  * @param backfill If this execution is for a backfill, the [[Backfill]] informations are provided.
  */
case class TimeSeriesContext(start: Instant,
                             end: Instant,
                             backfill: Option[Backfill] = None,
                             projectVersion: String = "")
    extends SchedulingContext {

  override def asJson: Json = TimeSeriesContext.encoder(this)
  def toId: String = {
    val priority = backfill.fold(0)(_.priority)
    val bytesPriority = BigInt(priority).toByteArray
    val paddedPriority: Array[Byte] = bytesPriority.reverse.padTo(10, '\u0000'.toByte).reverse
    s"${start}${paddedPriority.mkString}${UUID.randomUUID().toString}"
  }

  override def logIntoDatabase: ConnectionIO[String] = Database.serializeContext(this)

  def toInterval: Interval[Instant] = Interval(start, end)

  def compareTo(other: SchedulingContext) = other match {
    case TimeSeriesContext(otherStart, _, otherBackfill, _) =>
      val priority: (Option[Backfill] => Int) = _.map(_.priority).getOrElse(0)
      val thisBackfillPriority = priority(backfill)
      val otherBackfillPriority = priority(otherBackfill)
      if (thisBackfillPriority == otherBackfillPriority) {
        start.compareTo(otherStart)
      } else {
        thisBackfillPriority.compareTo(otherBackfillPriority)
      }
  }
}

object TimeSeriesContext {
  private[timeseries] implicit val encoder: Encoder[TimeSeriesContext] = deriveEncoder
  private[timeseries] implicit def decoder(implicit jobs: Set[Job[TimeSeries]]): Decoder[TimeSeriesContext] =
    deriveDecoder

  /** Provide an implicit `Ordering` for [[TimeSeriesContext]] based on the `compareTo` function. */
  implicit val ordering: Ordering[TimeSeriesContext] =
    Ordering.comparatorToOrdering(new Comparator[TimeSeriesContext] {
      def compare(o1: TimeSeriesContext, o2: TimeSeriesContext) = o1.compareTo(o2)
    })
}

/** A [[TimeSeriesDependency]] qualify the dependency between 2 [[com.criteo.cuttle.Job Jobs]] in a
  * [[TimeSeries]] [[com.criteo.cuttle.Workflow Workflow]]. It can be configured to `offset` the dependency.
  *
  * Supposing job1 depends on job2 with dependency descriptor (offsetLow, offsetHigh).
  * Then to execute period (low, high) of job1, we need period
  * (low+offsetLow, high+offsetHigh) of job2.
  *
  * @param offsetLow   the offset for the low end of the duration
  * @param offsetHigh  the offset for the high end of the duration
  *
  */
case class TimeSeriesDependency(offsetLow: Duration, offsetHigh: Duration)

/** Configure a [[com.criteo.cuttle.Job Job]] as a [[TimeSeries]] job,
  *
  * @param calendar The calendar partitions configuration for this job (for example hourly or daily).
  * @param start The start instant at which this job must start being executed.
  * @param maxPeriods The maximum number of partitions the job can handle at once. If this is defined
  *                   to a value more than `1` and if possible, the scheduler can trigger [[com.criteo.cuttle.Execution Executions]]
  *                   for more than 1 partition at once.
  */
case class TimeSeries(calendar: TimeSeriesCalendar, start: Instant, end: Option[Instant] = None, maxPeriods: Int = 1)
    extends Scheduling {
  type Context = TimeSeriesContext
  override def asJson: Json =
    Json.obj(
      "kind" -> "timeseries".asJson,
      "start" -> start.asJson,
      "end" -> end.asJson,
      "maxPeriods" -> maxPeriods.asJson,
      "calendar" -> calendar.asJson
    )
}

/** [[TimeSeries]] utilities. */
object TimeSeries {
  /* Provide a default [[TimeSeriesScheduler]] for [[TimeSeries]] scheduling. */
  implicit def scheduler(implicit logger: Logger) = TimeSeriesScheduler(logger)
}

private[timeseries] sealed trait JobState
private[timeseries] object JobState {
  case class Done(projectVersion: String) extends JobState
  case class Todo(maybeBackfill: Option[Backfill]) extends JobState
  case class Running(executionId: String) extends JobState

  implicit val doneDecoder: Decoder[Done] = new Decoder[Done] {
    final def apply(c: HCursor): Decoder.Result[Done] =
      for {
        version <- c.downField("projectVersion").as[String].orElse(Right("no-version"))
      } yield Done(version)
  }

  import TimeSeriesUtils._
  implicit val encoder: Encoder[JobState] = deriveEncoder
  implicit def decoder(implicit jobs: Set[TimeSeriesJob]): Decoder[JobState] = deriveDecoder
  implicit val eqInstance: Eq[JobState] = Eq.fromUniversalEquals[JobState]
}

/** A [[TimeSeriesScheduler]] executes the [[com.criteo.cuttle.Workflow Workflow]] for the
  * time partitions defined in a calendar. Each [[com.criteo.cuttle.Job Job]] defines how it mnaps
  * to the calendar (for example Hourly or Daily UTC), and the [[com.criteo.cuttle.Scheduler Scheduler]]
  * ensure that at least one [[com.criteo.cuttle.Execution Execution]] is created and successfully run
  * for each defined Job/Period.
  *
  * The scheduler also allow to [[Backfill]] already computed partitions. The [[Backfill]] can be recursive
  * or not and an audit log of backfills is kept.
  */
case class TimeSeriesScheduler(logger: Logger) extends Scheduler[TimeSeries] {
  import JobState.{Done, Running, Todo}
  import TimeSeriesUtils._

  override val name = "timeseries"

  override val allContexts = Database.sqlGetContextsBetween(None, None)

  private val _state = Ref(Map.empty[TimeSeriesJob, IntervalMap[Instant, JobState]])

  private val _backfills = Ref(Set.empty[Backfill])

  private val _pausedJobs = Ref(Set.empty[PausedJob])

  def pausedJobs(): Set[PausedJob] = atomic { implicit txn =>
    _pausedJobs()
  }

  private val queries = new Queries {
    val appLogger: Logger = logger
  }

  private[timeseries] def state: (State, Set[Backfill]) = atomic { implicit txn =>
    (_state(), _backfills())
  }

  private[timeseries] case class BackfillError(job: TimeSeriesJob, msg: String) {
    override def toString: String =
      s"""
         |Error during backfill creation.
         |Job id: ${job.id}
         |Job name: ${job.name}
         |Error: $msg
        """.stripMargin
  }

  private def assert(condition: Boolean, msg: String)(implicit job: TimeSeriesJob) =
    (if (condition) Right(true) else Left(BackfillError(job, msg))).right

  /**
    * @return the list of errors for the input backfill configuration, if any
    */
  private[timeseries] def validateBackfill(
    backfill: Backfill,
    states: Map[TimeSeriesUtils.TimeSeriesJob, IntervalMap[Instant, JobState]]
  ): Set[BackfillError] = {
    val start = backfill.start
    val end = backfill.end
    val validationByJob = backfill.jobs.map { implicit job =>
      for {
        _ <- assert(start.isBefore(end), s"The start date[$start] should be superior that end date[$end].")
        validIn <- {
          val interval = Interval(start, end)
          // collect all periods that are intersecting with [start, end]
          val interval2State = states.apply(job).intersect(interval).toList
          // in a Done state, and correct periodicity
          Right(
            interval2State
              .collect {
                case (Interval(Finite(lo), Finite(hi)), Done(_)) => (lo, hi)
              }
              .sortBy(_._1)
          ).right
        }
        _ <- assert(validIn.nonEmpty, s"There isn't any successful execution between start[$start] and end[$end].")
        _ <- assert(
          validIn.head._1 == start,
          s"The start date[${validIn.head._1}] of first successful execution doesn't equal to backfill start date" +
            s"[$start].")
        _ <- assert(
          validIn.last._2 == end,
          s"The end date[${validIn.last._2}] of last successful execution doesn't equal to backfill end date" +
            s"[$end].")
        valid <- assert(validIn.zip(validIn.tail).forall { case (prev, next) => prev._2 == next._1 },
                        "There are some unsuccessful intervals.")
      } yield valid
    }

    validationByJob.flatMap(_ match {
      case Left(error) => Some(error)
      case Right(_)    => None
    })
  }

  private def updateBackfillState(backfill: Backfill): Unit = atomic { implicit txn =>
    _backfills() = _backfills() + backfill
    _state() = _state() ++ backfill.jobs.map(job => {
      val newStart = job.scheduling.calendar.truncate(backfill.start)
      val newEnd = job.scheduling.calendar.ceil(backfill.end)
      job -> _state().apply(job).update(Interval(newStart, newEnd), Todo(Some(backfill)))
    })
  }

  /**
    * Launch backfills on a set of jobs on each subperiod that can be backfilled.
    * A job has to succeed at least once on a period to be backfillable on that same period.
    * The current executions of jobs with periods overlapping the backfills are cancelled.
    *
    * @returns a side-effect performing the requested backfill on success, an error otherwise
    *
    * @example Assume a backfill is requested over the period t1, t5 for the jobs = {job1, job2}
    *          For illustrative purposes, let the past execution state be as follows:
    *          <table>
    *            <tr>
    *              <th>Time</th> <th>t1</th> <th>t2</th> <th>t3</th> <th>t4</th> <th>t5</th>
    *            </tr>
    *            <tr>
    *              <td>job1</td> <td>OK</td> <td>OK</td> <td>KO</td> <td>OK</td> <td>OK</td>
    *            </tr>
    *            <tr>
    *              <td>job2</td> <td>OK</td> <td>OK</td> <td>KO</td> <td>KO</td> <td>OK</td>
    *            </tr>
    *          </table>
    *          Then the backfill will be lauched on {t1, t2} for jobs {job1, job2}, on {t4} for {job1} and on {t5} for {job1, job2}
    */
  private[timeseries] def backfillJob(name: String,
                                      description: String,
                                      jobs: Set[TimeSeriesJob],
                                      start: Instant,
                                      end: Instant,
                                      priority: Int,
                                      runningExecutions: TMap[Execution[TimeSeries], Future[Completed]],
                                      xa: XA)(implicit user: Auth.User): IO[Either[String, Unit]] = {

    logger.debug(s"Requesting a backfill of ${jobs.map(_.id)} between $start and $end")

    val result = atomic { implicit txn =>
      val currentJobStates = _state()
      val backfillErrors = mutable.ArrayBuffer.empty[String]
      val backfills = createBackfills(name, description, jobs, currentJobStates, start, end, priority)

      val validBackfills: List[Backfill] = backfills.flatMap { newBackfill =>
        validateBackfill(newBackfill, currentJobStates).toList match {
          case Nil => Some(newBackfill)
          case (validationErrors) =>
            backfillErrors += validationErrors.mkString("\n")
            None
        }
      }

      if (backfillErrors.isEmpty) Right(validBackfills)
      else Left(backfillErrors.toList)
    }

    result match {
      case Left(errors) => IO.pure(Left(errors.mkString("\n")))
      case Right(backfills) =>
        val dbUpdate: IO[Either[String, Unit]] = backfills
          .map { newBackfill =>
            updateBackfillState(newBackfill)
            Database.createBackfill(newBackfill).transact(xa)
          }
          .sequence
          .map(_ => Right(Unit))

        dbUpdate
    }
  }

  private[timeseries] def createBackfills(name: String,
                                          description: String,
                                          jobs: Set[TimeSeriesJob],
                                          states: Map[TimeSeriesUtils.TimeSeriesJob, IntervalMap[Instant, JobState]],
                                          start: Instant,
                                          end: Instant,
                                          priority: Int)(implicit user: Auth.User): List[Backfill] = {
    type TimeInterval = (Instant, Instant)

    val queryInterval = Interval(start, end)

    // Identify the jobs to backfill for each elementary period spanning the query interval

    // Find jobs which can be backfilled on the requested period. Those are the jobs whose state is 'Done'.
    val candidateBackfillsByPeriod: List[(TimeInterval, TimeSeriesJob)] = jobs.flatMap { job =>
      // collect all periods that are intersecting with [start, end]
      val interval2State: List[(Interval[Instant], JobState)] = states(job).intersect(queryInterval).toList
      // in a Done state, and correct periodicity
      interval2State.collect {
        case (Interval(Finite(lo), Finite(hi)), Done(_)) =>
          (lo, hi) -> job
      }
    }.toList

    val candidateBackfillsGroupedByPeriod: Map[TimeInterval, Set[TimeSeriesJob]] = candidateBackfillsByPeriod
      .groupBy { case (interval, job) => interval }
      .map { case (interval, groups) => interval -> groups.map { case (interval, job) => job }.toSet }

    val orderingByTimestamp = Ordering.by { e: Instant =>
      e.toEpochMilli
    }

    // Build an interval tree with the jobs to backfill for faster intersection queries
    var jobsByElementaryPeriod = RangedSeq.empty[(TimeInterval, Set[TimeSeriesJob]), Instant](_._1, orderingByTimestamp)

    candidateBackfillsGroupedByPeriod.foreach {
      case (interval, jobs) =>
        jobsByElementaryPeriod = jobsByElementaryPeriod + (interval -> jobs)
    }

    val elementaryPeriods: List[(Instant, Instant)] = candidateBackfillsByPeriod
      .flatMap { case (interval, _) => List(interval._1, interval._2) }
      .distinct
      .sorted
      .iterator
      .sliding(2)
      .withPartial(false)
      .toList
      .map { case List(start, end) => (start, end) }

    val jobsToBackFillByPeriod: List[(TimeInterval, Set[TimeSeriesJob])] = elementaryPeriods
      .map {
        case (start, end) =>
          val jobsOnPeriod = jobsByElementaryPeriod
            .filterOverlaps(start -> end)
            .map(_._2)
            .foldLeft(Set.empty[TimeSeriesJob])(_ ++ _)
          (start, end) -> jobsOnPeriod
      }
      .filter { case (interval, jobs) => jobs.nonEmpty }

    val backfills: List[Backfill] = jobsToBackFillByPeriod.map {
      case ((backfillStart, backfillEnd), jobsToBackfill) =>
        Backfill(
          UUID.randomUUID().toString,
          backfillStart,
          backfillEnd,
          jobsToBackfill,
          priority,
          name,
          description,
          "RUNNING",
          user.userId
        )
    }

    backfills
  }

  private[timeseries] def pauseJobs(jobs: Set[Job[TimeSeries]], executor: Executor[TimeSeries], xa: XA)(
    implicit user: Auth.User): Unit = {
    val executionsToCancel = atomic { implicit tx =>
      val pauseDate = Instant.now()
      val pausedJobIds = _pausedJobs().map(_.id)
      val jobsToPause: Set[PausedJob] = jobs
        .filter(job => !pausedJobIds.contains(job.id))
        .map(job => PausedJob(job.id, user, pauseDate))

      if (jobsToPause.isEmpty) return

      _pausedJobs() = _pausedJobs() ++ jobsToPause

      val pauseQuery = jobsToPause.map(queries.pauseJob).reduceLeft(_ *> _)
      Txn.setExternalDecider(new ExternalDecider {
        def shouldCommit(implicit txn: InTxnEnd): Boolean = {
          pauseQuery.transact(xa).unsafeRunSync
          true
        }
      })

      jobsToPause.flatMap { pausedJob =>
        executor.runningState.filterKeys(_.job.id == pausedJob.id).keys ++ executor.throttledState
          .filterKeys(_.job.id == pausedJob.id)
          .keys
      }
    }
    logger.debug(s"we will cancel ${executionsToCancel.size} executions")
    executionsToCancel.toList.sortBy(_.context).reverse.foreach { execution =>
      execution.streams.debug(s"Job has been paused by user ${user.userId}")
      execution.cancel()
    }
  }

  private[timeseries] def resumeJobs(jobs: Set[Job[TimeSeries]], xa: XA)(implicit user: Auth.User): Unit = {
    val jobIdsToResume = jobs.map(_.id)
    val resumeQuery = jobIdsToResume.map(queries.resumeJob).reduceLeft(_ *> _)

    atomic { implicit tx =>
      Txn.setExternalDecider(new ExternalDecider {
        def shouldCommit(implicit tx: InTxnEnd): Boolean = {
          resumeQuery.transact(xa).unsafeRunSync
          true
        }
      })

      _pausedJobs() = _pausedJobs() -- _pausedJobs().filter(pausedJob => jobIdsToResume.contains(pausedJob.id))
    }
  }

  /**
    * Given a list of current executions, update their state and submit new executions depending on the current time and
    * changes in execution states.
    * @param running set of still running executions
    * @return new set of running executions
    **/
  private[timeseries] def updateCurrentExecutionsAndSubmitNewExecutions(running: Set[Run],
                                                                        workflow: Workflow,
                                                                        executor: Executor[TimeSeries],
                                                                        xa: XA): Set[Run] = {
    val (completed, stillRunning) = running.partition {
      case (_, _, effect) => effect.isCompleted
    }

    val (stateSnapshot, completedBackfills, toRun) = atomic { implicit txn =>
      val (stateSnapshot, newBackfills, completedBackfills) =
        collectCompletedJobs(_state(), _backfills(), completed)

      val toRun = jobsToRun(workflow, stateSnapshot, Instant.now, executor.projectVersion)

      _state() = stateSnapshot
      _backfills() = newBackfills

      (stateSnapshot, completedBackfills, toRun)
    }

    val newExecutions = executor.runAll(toRun)

    atomic { implicit txn =>
      _state() = newExecutions.foldLeft(_state()) {
        case (st, (execution, _)) =>
          st + (execution.job ->
            st(execution.job).update(execution.context.toInterval, Running(execution.id)))
      }
    }

    if (completed.nonEmpty || toRun.nonEmpty)
      runOrLogAndDie(Database.serializeState(stateSnapshot).transact(xa).unsafeRunSync,
                     "TimeseriesScheduler, cannot serialize state, shutting down")

    if (completedBackfills.nonEmpty)
      runOrLogAndDie(
        Database
          .setBackfillStatus(completedBackfills.map(_.id), "COMPLETE")
          .transact(xa)
          .unsafeRunSync,
        "TimeseriesScheduler, cannot serialize state, shutting down"
      )

    val newRunning = stillRunning ++ newExecutions.map {
      case (execution, result) =>
        (execution.job, execution.context, result)
    }
    newRunning
  }

  private[timeseries] def initialize(workflow0: Workload[TimeSeries], xa: XA, logger: Logger) = {
    val workflow = workflow0.asInstanceOf[Workflow]
    logger.info("Validate workflow before start")
    TimeSeriesUtils.validate(workflow) match {
      case Left(errors) =>
        val consolidatedError = errors.mkString("\n")
        logger.error(consolidatedError)
        throw new IllegalArgumentException(consolidatedError)
      case Right(_) => ()
    }
    logger.info("Workflow is valid")

    logger.info("Applying migrations to database")
    Database.doSchemaUpdates.transact(xa).unsafeRunSync
    logger.info("Database up-to-date")

    Database
      .deserializeState(workflow.vertices)
      .transact(xa)
      .unsafeRunSync
      .foreach { state =>
        atomic { implicit txn =>
          _state() = cleanTimeseriesState(state)
        }
      }

    atomic { implicit txn =>
      val incompleteBackfills = Database
        .queryBackfills(Some(sql"""status = 'RUNNING'"""))
        .to[List]
        .map(_.map {
          case (id, name, description, jobsIdsString, priority, start, end, _, status, createdBy) =>
            val jobsIds = jobsIdsString.split(",")
            val jobs = workflow.vertices.filter { job =>
              jobsIds.contains(job.id)
            }
            Backfill(id, start, end, jobs, priority, name, description, status, createdBy)
        })
        .transact(xa)
        .unsafeRunSync

      _backfills() = _backfills() ++ incompleteBackfills
      _pausedJobs() = _pausedJobs() ++ queries.getPausedJobs.transact(xa).unsafeRunSync()

      workflow.vertices.foreach { job =>
        val calendar = job.scheduling.calendar
        val definedInterval = Interval(Finite(calendar.ceil(job.scheduling.start)),
                                       job.scheduling.end.map(calendar.truncate _).map(Finite.apply _).getOrElse(Top))
        val oldJobState = _state().getOrElse(job, IntervalMap.empty[Instant, JobState])
        val missingIntervals = IntervalMap(definedInterval -> (()))
          .whenIsUndef(oldJobState.intersect(definedInterval))
          .toList
          .map(_._1)
        val jobState = missingIntervals
          .foldLeft(oldJobState) { (st, interval) =>
            st.update(interval, Todo(None))
          }
          .intersect(definedInterval)
        _state() = _state() + (job -> jobState)
      }
    }
    workflow
  }

  def start(workflow0: Workload[TimeSeries], executor: Executor[TimeSeries], xa: XA, logger: Logger): Unit = {
    val workflow = initialize(workflow0, xa, logger)

    def mainLoop(running: Set[Run]): Unit = {
      val newRunning = updateCurrentExecutionsAndSubmitNewExecutions(running, workflow, executor, xa)
      utils.Timeout(ScalaDuration.create(1, "s")).andThen { case _ => mainLoop(newRunning) }
    }

    mainLoop(Set.empty)
  }

  private def runOrLogAndDie(thunk: => Unit, message: => String): Unit = {
    import java.io._

    try {
      thunk
    } catch {
      case (e: Throwable) => {
        logger.error(message)
        val sw = new StringWriter
        e.printStackTrace(new PrintWriter(sw))
        logger.error(sw.toString)
        System.exit(-1)
      }
    }
  }

  private[timeseries] def collectCompletedJobs(state: State,
                                               backfills: Set[Backfill],
                                               completed: Set[Run]): (State, Set[Backfill], Set[Backfill]) = {
    def isDone(state: State, job: TimeSeriesJob, context: TimeSeriesContext): Boolean =
      state.apply(job).intersect(context.toInterval).toList.forall {
        case (_, Done(_)) => true
        case _            => false
      }

    // update state with job statuses
    val newState = completed.foldLeft(state) {
      case (acc, (job, context, future)) =>
        val jobState =
          if (future.value.get.isSuccess || isDone(state, job, context)) Done(context.projectVersion)
          else Todo(context.backfill)
        acc + (job -> acc(job).update(context.toInterval, jobState))
    }

    def jobHasExecutionsRunningOnPeriod(job: Job[TimeSeries], period: Interval[Instant]): Boolean = {
      val jobStateOnPeriod = newState(job).intersect(period).toList
      jobStateOnPeriod.exists {
        case (interval, jobState) =>
          jobState match {
            case Done(_) => false
            case _       => true
          }
      }
    }

    val notCompletedBackfills = backfills.filter { bf =>
      val itvl = Interval(bf.start, bf.end)
      bf.jobs.exists(job => jobHasExecutionsRunningOnPeriod(job, itvl))
    }

    (newState, notCompletedBackfills, backfills -- notCompletedBackfills)
  }

  private[timeseries] def jobsToRun(workflow: Workflow,
                                    state0: State,
                                    now: Instant,
                                    projectVersion: String): List[Executable] = {

    val timerInterval = Interval(Bottom, Finite(now))
    val state = state0.mapValues(_.intersect(timerInterval))

    val parentsMap = workflow.edges.groupBy { case (child, _, _)   => child }
    val childrenMap = workflow.edges.groupBy { case (_, parent, _) => parent }
    val pausedJobIds = pausedJobs().map(_.id)

    def reverseDescr(dep: TimeSeriesDependency) =
      TimeSeriesDependency(dep.offsetLow.negated, dep.offsetHigh.negated)
    def applyDep(dep: TimeSeriesDependency): PartialFunction[Interval[Instant], Interval[Instant]] =
      Function.unlift { (i: Interval[Instant]) =>
        val low = i.lo.map(_.plus(dep.offsetLow))
        val high = i.hi.map(_.plus(dep.offsetHigh))
        if (low >= high) None else Some(Interval(low, high))
      }

    def joinIntervals(intervals: List[Interval[Instant]]): List[Interval[Instant]] =
      intervals
        .foldLeft(IntervalMap.empty[Instant, Unit]) { case (intervalMap, interval) => intervalMap.update(interval, ()) }
        .toList
        .map { case (interval, _) => interval }

    workflow.vertices.filter(job => !pausedJobIds.contains(job.id)).toList.flatMap { job =>
      val full = IntervalMap[Instant, Unit](Interval[Instant](Bottom, Top) -> (()))
      val dependenciesSatisfied = parentsMap
        .getOrElse(job, Set.empty)
        .map {
          case (_, parent, lbl) =>
            val donePeriods: IntervalMap[Instant, Unit] = state(parent).collect { case Done(_) => () }
            val intervals: List[Interval[Instant]] =
              joinIntervals(donePeriods.toList.map { case (interval, _) => interval })
            val newIntervals = intervals.collect(applyDep(reverseDescr(lbl)))
            val intervalMapOfSatisfiedDeps = newIntervals.foldLeft(IntervalMap.empty[Instant, Unit])(_.update(_, ()))
            intervalMapOfSatisfiedDeps
        }
        .fold(full)(_ whenIsDef _)

      val noChildrenRunning = childrenMap
        .getOrElse(job, Set.empty)
        .map {
          case (child, _, lbl) =>
            val runningPeriods: IntervalMap[Instant, Unit] = state(child).collect { case Running(_) => () }
            val intervals = joinIntervals(runningPeriods.toList.map { case (interval, _) => interval })
            val newIntervals = intervals.collect(applyDep(lbl))
            val intervalMapWithCompletedChildren =
              newIntervals.foldLeft(IntervalMap.empty[Instant, Unit])(_.update(_, ()))
            intervalMapWithCompletedChildren
        }
        .fold(full)(_ whenIsUndef _)

      val todoPeriods: IntervalMap[Instant, Option[Backfill]] = state(job).collect {
        case Todo(maybeBackfill) => maybeBackfill
      }
      val toRun = todoPeriods
        .whenIsDef(dependenciesSatisfied)
        .whenIsDef(noChildrenRunning)

      for {
        (interval, maybeBackfill) <- toRun.toList
        (lo, hi) <- job.scheduling.calendar.inInterval(interval, job.scheduling.maxPeriods)
      } yield {
        (job, TimeSeriesContext(lo, hi, maybeBackfill, projectVersion))
      }
    }
  }

  private[timeseries] def forceSuccess(job: TimeSeriesJob, interval: Interval[Instant], projectVersion: String): Unit =
    atomic { implicit txn =>
      val jobState = _state().apply(job)
      _state() = _state() + (job -> jobState.update(interval, Done(projectVersion)))
    }

  private def getRunningBackfillsSize(jobs: Set[String]) = {
    val runningBackfills = state match {
      case (_, backfills) =>
        backfills.filter(
          bf =>
            bf.status == "RUNNING" &&
              bf.jobs.map(_.id).intersect(jobs).nonEmpty)
    }

    runningBackfills.size
  }

  /**
    * We compute the last instant when job was in a valid state
    * @param jobs set of jobs to process
    * @return Iterable of job to to last instant when job was in a valid state.
    *         Iterable is empty when job doesn't contain any "DONE" interval.
    */
  private def getTimeOfLastSuccess(jobs: Set[String]) =
    _state
      .single()
      .collect {
        case (job, intervals) if jobs.contains(job.id) =>
          val intervalList = intervals.toList
          val lastValidInterval = intervalList.takeWhile {
            case (_, Running(_)) => false
            case (_, Todo(None)) => false
            case _               => true
          }.lastOption

          lastValidInterval.map {
            case (interval, _) =>
              job -> (interval.hi match {
                case Finite(instant) => instant
                case _               => Instant.MAX
              })
          }
      }
      .flatten
      .toSeq

  override def getMetrics(jobIds: Set[String], workflow0: Workload[TimeSeries]): Seq[Metric] = {
    val workflow = workflow0.asInstanceOf[Workflow]
    val lastSuccessTime = getTimeOfLastSuccess(jobIds)
    val secondsSinceLastSuccess = lastSuccessTime.foldLeft(
      Gauge(
        "cuttle_timeseries_scheduler_last_success_epoch_seconds",
        "The seconds since a job's last success with all previous executions being successful"
      )
    ) {
      case (gauge, (job, lastSuccess)) =>
        val tags = if (!job.tags.isEmpty) Set("tags" -> job.tags.map(_.name).mkString(",")) else Nil
        gauge.labeled(
          Set("job_id" -> job.id, "job_name" -> job.name) ++ tags,
          Instant.now.getEpochSecond - lastSuccess.getEpochSecond
        )
    }

    val absoluteLatency = lastSuccessTime.map {
      case (job, lastSuccess) => job -> getAbsoluteLatency(job, lastSuccess)
    }.toMap

    val latencies = List(
      Gauge(
        "cuttle_timeseries_scheduler_absolute_latency_epoch_seconds",
        "Absolute latency of a job in seconds, with respect to its last success with all previous executions being successful"
      ) -> absoluteLatency,
      Gauge(
        "cuttle_timeseries_scheduler_relative_latency_epoch_seconds",
        "Relative latency of a job in seconds, taking into account its parents' latencies"
      ) -> absoluteLatency.map {
        case (job, latency) =>
          val maxParentLatency = workflow.edges
            .collect { case (v, dep, _) if v == job => dep }
            .flatMap(absoluteLatency.get)
            .foldLeft(0L)(Math.max)
          job -> Math.max(0, latency - maxParentLatency)
      }
    ).map {
      case (gauge, latencyValues) =>
        latencyValues.foldLeft(gauge) {
          case (gauge, (job, latency)) =>
            val tags = if (!job.tags.isEmpty) Set("tags" -> job.tags.map(_.name).mkString(",")) else Nil
            gauge.labeled(Set("job_id" -> job.id, "job_name" -> job.name) ++ tags, latency)
        }
    }

    Seq(
      Gauge("cuttle_timeseries_scheduler_stat_count", "The number of backfills")
        .labeled("type" -> "backfills", getRunningBackfillsSize(jobIds)),
      secondsSinceLastSuccess
    ) ++ latencies
  }

  override def getStats(jobs: Set[String]): Json =
    Map("backfills" -> getRunningBackfillsSize(jobs)).asJson

  private def getAbsoluteLatency(job: TimeSeriesJob, lastSuccess: Instant): Long = {
    val expectedSuccess = job.scheduling.calendar match {
      case Hourly        => Instant.now.minus(1, HOURS)
      case Daily(tz)     => Instant.now.atZone(tz).minus(1, DAYS).toInstant
      case Weekly(tz, _) => Instant.now.atZone(tz).minus(1, WEEKS).toInstant
      case Monthly(tz)   => Instant.now.atZone(tz).minus(1, MONTHS).toInstant
    }
    if (expectedSuccess.compareTo(lastSuccess) <= 0) 0L else expectedSuccess.getEpochSecond - lastSuccess.getEpochSecond
  }
}

object TimeSeriesUtils {
  type TimeSeriesJob = Job[TimeSeries]
  type Executable = (TimeSeriesJob, TimeSeriesContext)
  type Run = (TimeSeriesJob, TimeSeriesContext, Future[Completed])
  type State = Map[TimeSeriesJob, IntervalMap[Instant, JobState]]

  val UTC: ZoneId = ZoneId.of("UTC")

  /**
    * Validation of:
    * - absence of cycles in the workflow, implemented based on Kahn's algorithm
    * - absence of (parent job, child job) dependencies where child has a start date before parent's start date
    * - absence of jobs with the same id
    * @param workflow workflow to be validated
    * @return either a validation errors list or a unit
    */
  def validate(workflow: Workflow): Either[List[String], Unit] = {
    val errors = collection.mutable.ListBuffer(Workflow.validate(workflow): _*)

    workflow.edges.map {
      case (childJob, parentJob, _) =>
        if (childJob.scheduling.start.isBefore(parentJob.scheduling.start)) {
          errors += s"Job [${childJob.id}] starts at [${childJob.scheduling.start.toString}] " +
            s"before his parent [${parentJob.id}] at [${parentJob.scheduling.start.toString}]"
        }
    }

    if (errors.nonEmpty) Left(errors.toList)
    else Right(())
  }

  /**
    * Filter potentially incorrect state intervals which were the result of bugs in previous versions. These bugs were mainly
    * related to the addition of the project version in the serialized execution state.
    */
  private[timeseries] def cleanTimeseriesState(state: State): State = {
    val cleanedState = state.map { case (job, intervalMap) =>
      job -> intervalMap.toList
        .foldLeft(IntervalMap.empty[Instant, JobState]) { case (aggregatedIntervals, (interval, jobState)) =>
          aggregatedIntervals.update(interval, jobState)
        }
    }
    cleanedState
  }
}
