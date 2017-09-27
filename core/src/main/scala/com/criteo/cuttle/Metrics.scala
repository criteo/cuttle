package com.criteo.cuttle

import scala.concurrent.stm.{atomic, Ref}

sealed trait Metric {
  def toString: String
}

case class Gauge(name: String, compute: () => Long, tags: Seq[(String, String)] = Seq.empty) extends Metric
case class Comment(text: String) extends Metric
class GenericMetric extends Metric

trait MetricRepository {
  def addMetric(metric: Metric): MetricRepository

  def toString: String
}

class PrometheusRepository(initMetrics: Seq[Metric] = Seq.empty) extends MetricRepository {
  val metrics = Ref(initMetrics)

  override def addMetric(metric: Metric): PrometheusRepository = atomic { implicit txn =>
    metrics() = metrics() :+ metric
    this
  }

  override def toString: String = {
    val prometheusMetrics = metrics.single().map {
      case Gauge(name, compute, tags) =>
        s"$name {${if (tags.nonEmpty) tags.map(tag => s"""${tag._1}="${tag._2}"""").mkString(", ") else ""}} ${compute()}"
      case Comment(text)    => s"# $text"
      case m: GenericMetric => m.toString
    }

    s"${prometheusMetrics.mkString("\n")}\n"
  }
}

object PrometheusRepository {
  def apply(metric: Metric): PrometheusRepository = new PrometheusRepository(Seq(metric))
  def apply(): PrometheusRepository = new PrometheusRepository()
}
