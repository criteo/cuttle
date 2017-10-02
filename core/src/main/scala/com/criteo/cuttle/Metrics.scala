package com.criteo.cuttle

import scala.collection.mutable.{ Map => MMap }

object Metrics {

  object MetricType extends Enumeration {
    type MetricType = Value
    val gauge = Value
  }

  import MetricType._

  sealed trait Metric {
    val name: String
    val help: String
    val metricType: MetricType
    val labels2Value: MMap[Set[(String, String)], AnyVal]

    def isDefined: Boolean = labels2Value.nonEmpty
  }

  case class Gauge(name: String, help: String = "") extends Metric {

    override val metricType: MetricType = gauge
    override val labels2Value: MMap[Set[(String, String)], AnyVal] = MMap.empty

    def labeled(labels: Set[(String, String)], value: AnyVal): Gauge = {
      labels2Value += labels -> value
      this
    }

    def labeled(label: (String, String), value: AnyVal): Gauge = {
      labels2Value += Set(label) -> value
      this
    }

    def set(value: AnyVal): Gauge = {
      labels2Value += Set.empty[(String, String)] -> value
      this
    }

  }

  trait MetricProvider {
    def getMetrics(jobs: Set[String]): Seq[Metric]
  }

  private[cuttle] object Prometheus {
    private def serialize2PrometheusStrings[T](metric: Metric): Seq[String] = if (metric.isDefined) {
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