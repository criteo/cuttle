package com.criteo.cuttle

object Metrics {
  sealed trait Metric {
    def toString: String
  }

  case class Gauge(name: String, value: Long, tags: Seq[(String, String)] = Seq.empty) extends Metric

  trait MetricProvider {
    def getMetrics(jobs: Set[String]): Seq[Metric]
  }

  private[cuttle] object Prometheus {
    def format(metrics: Seq[Metric]): String = {
      val prometheusMetrics = metrics.map {
        case Gauge(name, value, tags) =>
          s"$name${if (tags.nonEmpty) s" {${tags.map(tag => s"""${tag._1}="${tag._2}"""").mkString(", ")}}"
          else ""} $value"
      }

      s"${prometheusMetrics.mkString("\n")}\n"
    }
  }
}
