package com.criteo.cuttle

/** Expose cuttle metrics via the [[https://prometheus.io prometheus]] protocol. */
object Metrics {

  object MetricType extends Enumeration {
    type MetricType = Value
    val gauge = Value
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

  /** A __Gauge__ metric epresents a single numerical value that can arbitrarily go up and down.
    *
    * @param name The metric name.
    * @param help Metric description if provided.
    * @param labels2Value The current value.s
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
