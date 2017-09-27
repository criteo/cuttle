package com.criteo.cuttle

sealed trait Metric {
  def toString: String
}

case class Gauge(name: String, value: Long, tags: Seq[(String, String)] = Seq.empty) extends Metric

trait MetricProvider {
  private[cuttle] def getMetrics(jobs: Set[String]): Seq[Metric]
}

object Prometheus {
  def format(metrics: Seq[Metric]): String = {
    val prometheusMetrics = metrics.map {
      case Gauge(name, value, tags) =>
        s"$name {${if (tags.nonEmpty) tags.map(tag => s"""${tag._1}="${tag._2}"""").mkString(", ") else ""}} $value"
    }

    s"${prometheusMetrics.mkString("\n")}\n"
  }
}