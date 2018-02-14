package com.criteo.cuttle

import scala.concurrent.stm.{atomic, TMap}
import scala.math.Numeric

/** Expose cuttle metrics via the [[https://prometheus.io prometheus]] protocol. */
object Metrics {

  object MetricType extends Enumeration {
    type MetricType = Value
    val gauge = Value
    val counter = Value
  }

  import MetricType._

  /** A metric to be exposed. */
  sealed trait Metric {
    val name: String
    val help: String
    val metricType: MetricType
    val labels2Value: Map[Set[(String, String)], AnyVal]

    def isDefined: Boolean = labels2Value.nonEmpty
  }

  /** A __Gauge__ metric represents a single numerical value that can arbitrarily go up and down.
    *
    * @param name The metric name.
    * @param help Metric description if provided.
    * @param labels2Value map of (label name, label value) pairs to the current gauge values
    */
  case class Gauge(name: String, help: String = "", labels2Value: Map[Set[(String, String)], AnyVal] = Map.empty)
      extends Metric {

    override val metricType: MetricType = gauge

    def labeled(labels: Set[(String, String)], value: AnyVal): Gauge =
      copy(labels2Value = labels2Value + (labels -> value))

    def labeled(label: (String, String), value: AnyVal): Gauge =
      copy(labels2Value = labels2Value + (Set(label) -> value))

    def set(value: AnyVal): Gauge = copy(labels2Value = labels2Value + (Set.empty[(String, String)] -> value))
  }

  /**
    * A __Counter__ contains a value that can only be incremented. Increments are not threadsafe.
    *
    * @param name The metric name.
    * @param help Metric description if provided.
    * @param labels2Value map of (label name, label value) pairs to counter values
    */
  case class Counter[T](
    name: String,
    help: String = "",
    labels2Value: Map[Set[(String, String)], AnyVal] = Map.empty
  )(implicit number: Numeric[T])
      extends Metric {

    override val metricType: MetricType = counter

    def inc(label: Set[(String, String)]): Counter[T] = {
      val currentCount = labels2Value.getOrElse(label, number.zero).asInstanceOf[T]
      copy(labels2Value = labels2Value + (label -> number.plus(currentCount, number.one).asInstanceOf[AnyVal]))
    }
  }

  /** Components able to provide metrics. */
  trait MetricProvider[S <: Scheduling] {
    def getMetrics(jobs: Set[String], workflow: Workflow[S]): Seq[Metric]
  }

  private[cuttle] object Prometheus {
    private def serialize2PrometheusStrings[T](metric: Metric): Seq[String] =
      if (metric.isDefined) {
        val labeledMetrics = metric.labels2Value.map {
          case (labels, value) =>
            val labelsSerialized =
              if (labels.nonEmpty) s" {${labels.map(label => s"""${label._1}="${label._2}"""").mkString(", ")}} "
              else " "
            s"${metric.name}$labelsSerialized$value"
        }

        val metricType = s"# TYPE ${metric.name} ${metric.metricType}"
        val help = if (metric.help.nonEmpty) {
          Seq(s"# HELP ${metric.name} ${metric.help}")
        } else Seq.empty[String]

        (help :+ metricType) ++ labeledMetrics
      } else Seq.empty

    def serialize(metrics: Seq[Metric]): String = s"${metrics.flatMap(serialize2PrometheusStrings).mkString("\n")}\n"
  }

}
