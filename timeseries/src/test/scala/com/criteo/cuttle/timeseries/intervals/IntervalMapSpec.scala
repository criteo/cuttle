package com.criteo.cuttle.timeseries.intervals

import cats.implicits._

import org.scalatest.FunSuite

class IntervalMapSpec extends FunSuite {
  test("intervals") {
    assert(
      IntervalMap(Interval(0, 3) -> 42, Interval(3, 5) -> 12) ==
      IntervalMap(Interval(0, 3) -> 42, Interval(3, 5) -> 12)
    )
  }

  test("broken intervals") {
    intercept[Exception](
      IntervalMap(Interval(0, 3) -> 42, Interval(3, 5) -> 42)
    )
  }

  test("broken intervals 2") {
    intercept[Exception](
      IntervalMap(Interval(0, 3) -> 42, Interval(2, 5) -> 13)
    )
  }

  test("merge intervals") {
    val intervals = IntervalMap(Interval(0, 3) -> 42, Interval(3, 5) -> 12)
    assert(
      intervals.map(_ => 1) ==
      IntervalMap(Interval(0, 5) -> 1)
    )
  }

  test("whenUndef") {
    assert(
      IntervalMap(Interval(0, 3) -> 42)
        .whenIsUndef(IntervalMap(Interval(1, 2) -> "foo", Interval(2, 3) -> "bar")) ==
        IntervalMap(Interval(0, 1) -> 42))
  }
}
