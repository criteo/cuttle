package com.criteo.cuttle.timeseries

import cats.kernel.Eq

import algebra.lattice.Bool

import java.time.Instant

import continuum.{Interval, IntervalSet}

import TimeSeriesUtils._

case class StateD(defined: Map[TimeSeriesJob, IntervalSet[Instant]],
                  default: IntervalSet[Instant] = IntervalSet.empty) {
  def get(job: TimeSeriesJob) = defined.getOrElse(job, default)
}
object StateD {
  implicit def stateLattice: Bool[StateD] = new LatticeStateD
  implicit val eqInstance: Eq[StateD] = new Eq[StateD] {
    def eqv(x: StateD, y: StateD) =
      (x.defined.keySet union y.defined.keySet).forall { k =>
        x.get(k) == y.get(k)
      }
  }
}

class LatticeStateD extends Bool[StateD] {
  def or(x: StateD, y: StateD) = {
    val orMap = (x.defined.keySet union y.defined.keySet).map { k =>
      k -> (x.get(k) ++ y.get(k))
    }.toMap
    StateD(orMap, x.default ++ y.default)
  }
  def and(x: StateD, y: StateD) = {
    val andMap = (x.defined.keySet union y.defined.keySet).map { k =>
      k -> (x.get(k) intersect y.get(k))
    }.toMap
    StateD(andMap, x.default intersect y.default)
  }
  val zero = StateD(Map.empty, IntervalSet.empty)
  val one = StateD(Map.empty, IntervalSet(Interval.full))

  def complement(x: StateD) = StateD(x.defined.mapValues(_.complement), x.default.complement)
}
