package com.criteo.cuttle.timeseries.intervals

import org.scalatest.FunSuite

class IntervalMapSpec extends FunSuite {
  test("whenUndef") {
    assert(
      IntervalMap(Interval(0, 3) -> 42)
        .whenIsUndef(IntervalMap(Interval(1, 2) -> "foo", Interval(2, 3) -> "bar"))
        .toList ==
        IntervalMap(Interval(0, 1) -> 42).toList)
  }
}
