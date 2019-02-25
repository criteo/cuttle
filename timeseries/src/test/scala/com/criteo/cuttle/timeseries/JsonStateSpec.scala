package com.criteo.cuttle.timeseries

import com.criteo.cuttle.timeseries.intervals._
import Bound._

import TimeSeriesUtils.State

import java.time._

import org.scalatest.FunSuite

import com.criteo.cuttle._

import io.circe._
import io.circe.parser._
import cats.syntax.either._

import scala.concurrent._

class JsonStateSpec extends FunSuite with TestScheduling {

  val world = Job("world", hourly(Instant.now), "World")(_ => Future.successful(Completed))
  val hello1 = Job("hello1", hourly(Instant.now), "Hello 1")(_ => Future.successful(Completed))
  val hello2 = Job("hello2", hourly(Instant.now), "Hello 2")(_ => Future.successful(Completed))
  val hello3 = Job("hello3", hourly(Instant.now), "Hello 3")(_ => Future.successful(Completed))

  implicit val jobs = Set(world, hello1, hello2, hello3)

  val backfill = Backfill(
    "98396503-b437-4b0b-96d5-2a4f87281c47",
    date"2019-02-15T00:00:00Z",
    date"2019-02-16T00:00:00Z",
    jobs,
    0,
    "lol",
    "",
    "RUNNING",
    "Guest"
  )

  implicit val backfills = List(backfill)

  val legacyState = parse(
    """[
      |  [
      |    "world",
      |    [
      |      [
      |        {
      |          "lo" : {
      |            "Finite" : {
      |              "bound" : "2019-02-15T00:00:00Z"
      |            }
      |          },
      |          "hi" : {
      |            "Finite" : {
      |              "bound" : "2019-02-16T00:00:00Z"
      |            }
      |          }
      |        },
      |        {
      |          "Todo" : {
      |            "maybeBackfill" : {
      |              "id" : "98396503-b437-4b0b-96d5-2a4f87281c47",
      |              "start" : "2019-02-15T00:00:00Z",
      |              "end" : "2019-02-16T00:00:00Z",
      |              "jobs" : [
      |                "hello2",
      |                "hello1",
      |                "world",
      |                "hello3"
      |              ],
      |              "priority" : 0,
      |              "name" : "lol",
      |              "description" : "",
      |              "status" : "RUNNING",
      |              "createdBy" : "Guest"
      |            }
      |          }
      |        }
      |      ],
      |      [
      |        {
      |          "lo" : {
      |            "Finite" : {
      |              "bound" : "2019-02-16T00:00:00Z"
      |            }
      |          },
      |          "hi" : {
      |            "Top" : {
      |            }
      |          }
      |        },
      |        {
      |          "Todo" : {
      |            "maybeBackfill" : null
      |          }
      |        }
      |      ]
      |    ]
      |  ],
      |  [
      |    "hello1",
      |    [
      |      [
      |        {
      |          "lo" : {
      |            "Finite" : {
      |              "bound" : "2019-02-15T00:00:00Z"
      |            }
      |          },
      |          "hi" : {
      |            "Finite" : {
      |              "bound" : "2019-02-16T00:00:00Z"
      |            }
      |          }
      |        },
      |        {
      |          "Done" : {
      |            "projectVersion" : "456"
      |          }
      |        }
      |      ],
      |      [
      |        {
      |          "lo" : {
      |            "Finite" : {
      |              "bound" : "2019-02-16T00:00:00Z"
      |            }
      |          },
      |          "hi" : {
      |            "Finite" : {
      |              "bound" : "2019-02-22T12:00:00Z"
      |            }
      |          }
      |        },
      |        {
      |          "Done" : {
      |            "projectVersion" : "123"
      |          }
      |        }
      |      ],
      |      [
      |        {
      |          "lo" : {
      |            "Finite" : {
      |              "bound" : "2019-02-22T12:00:00Z"
      |            }
      |          },
      |          "hi" : {
      |            "Top" : {
      |            }
      |          }
      |        },
      |        {
      |          "Todo" : {
      |            "maybeBackfill" : null
      |          }
      |        }
      |      ]
      |    ]
      |  ],
      |  [
      |    "hello2",
      |    [
      |      [
      |        {
      |          "lo" : {
      |            "Finite" : {
      |              "bound" : "2019-02-15T00:00:00Z"
      |            }
      |          },
      |          "hi" : {
      |            "Finite" : {
      |              "bound" : "2019-02-15T05:00:00Z"
      |            }
      |          }
      |        },
      |        {
      |          "Done" : {
      |            "projectVersion" : "456"
      |          }
      |        }
      |      ],
      |      [
      |        {
      |          "lo" : {
      |            "Finite" : {
      |              "bound" : "2019-02-16T00:00:00Z"
      |            }
      |          },
      |          "hi" : {
      |            "Finite" : {
      |              "bound" : "2019-02-16T16:00:00Z"
      |            }
      |          }
      |        },
      |        {
      |          "Done" : {
      |            "projectVersion" : "456"
      |          }
      |        }
      |      ],
      |      [
      |        {
      |          "lo" : {
      |            "Finite" : {
      |              "bound" : "2019-02-22T12:00:00Z"
      |            }
      |          },
      |          "hi" : {
      |            "Top" : {
      |            }
      |          }
      |        },
      |        {
      |          "Todo" : {
      |            "maybeBackfill" : null
      |          }
      |        }
      |      ]
      |    ]
      |  ],
      |  [
      |    "hello3",
      |    [
      |      [
      |        {
      |          "lo" : {
      |            "Finite" : {
      |              "bound" : "2019-02-15T00:00:00Z"
      |            }
      |          },
      |          "hi" : {
      |            "Finite" : {
      |              "bound" : "2019-02-15T11:00:00Z"
      |            }
      |          }
      |        },
      |        {
      |          "Done" : {
      |            "projectVersion" : "456"
      |          }
      |        }
      |      ],
      |      [
      |        {
      |          "lo" : {
      |            "Finite" : {
      |              "bound" : "2019-02-16T00:00:00Z"
      |            }
      |          },
      |          "hi" : {
      |            "Finite" : {
      |              "bound" : "2019-02-16T16:00:00Z"
      |            }
      |          }
      |        },
      |        {
      |          "Done" : {
      |            "projectVersion" : "456"
      |          }
      |        }
      |      ],
      |      [
      |        {
      |          "lo" : {
      |            "Finite" : {
      |              "bound" : "2019-02-22T12:00:00Z"
      |            }
      |          },
      |          "hi" : {
      |            "Top" : {
      |            }
      |          }
      |        },
      |        {
      |          "Todo" : {
      |            "maybeBackfill" : null
      |          }
      |        }
      |      ]
      |    ]
      |  ]
      |]""".stripMargin).getOrElse(Json.Null)

  def checkState(state: State) = {
    val worldIntervals = state(world).toList
    assert(worldIntervals.size == 2)

    {
      val (interval, jobState) = worldIntervals(0)
      assert(interval == Interval(Finite(date"2019-02-15T00:00:00Z"), Finite(date"2019-02-16T00:00:00Z")))
      assert(jobState.isInstanceOf[JobState.Todo])
      assert(jobState.asInstanceOf[JobState.Todo].maybeBackfill.isDefined)
      assert(jobState.asInstanceOf[JobState.Todo].maybeBackfill.get.id == "98396503-b437-4b0b-96d5-2a4f87281c47")
      assert(jobState.asInstanceOf[JobState.Todo].maybeBackfill.get.name == "lol")
      assert(jobState.asInstanceOf[JobState.Todo].maybeBackfill.get.jobs.size == 4)
    }

    {
      val (interval, jobState) = worldIntervals(1)
      assert(interval == Interval(Finite(date"2019-02-16T00:00:00Z"), Top))
      assert(jobState.isInstanceOf[JobState.Todo])
      assert(jobState.asInstanceOf[JobState.Todo].maybeBackfill.isEmpty)
    }

    val  hello1Intervals = state(hello1).toList
    assert(hello1Intervals.size == 3)

    {
      val (interval, jobState) = hello1Intervals(0)
      assert(interval == Interval(Finite(date"2019-02-15T00:00:00Z"), Finite(date"2019-02-16T00:00:00Z")))
      assert(jobState.isInstanceOf[JobState.Done])
      assert(jobState.asInstanceOf[JobState.Done].projectVersion == "456")
    }

    {
      val (interval, jobState) = hello1Intervals(1)
      assert(interval == Interval(Finite(date"2019-02-16T00:00:00Z"), Finite(date"2019-02-22T12:00:00Z")))
      assert(jobState.isInstanceOf[JobState.Done])
      assert(jobState.asInstanceOf[JobState.Done].projectVersion == "123")
    }

      {
      val (interval, jobState) = hello1Intervals(2)
      assert(interval == Interval(Finite(date"2019-02-22T12:00:00Z"), Top))
      assert(jobState.isInstanceOf[JobState.Todo])
      assert(jobState.asInstanceOf[JobState.Todo].maybeBackfill.isEmpty)
    }
  }

  test("decode legacy state properly") {
    val legacyDecodedState = Database.dbStateDecoder(legacyState)
    assert(legacyDecodedState.isDefined)
    checkState(legacyDecodedState.get)
  }

  test("re-encode legacy state with new encoder") {
    val legacyDecodedState = Database.dbStateDecoder(legacyState)
    assert(legacyDecodedState.isDefined)
    checkState(legacyDecodedState.get)

    val newEncodedState = Database.dbStateEncoder(legacyDecodedState.get)

    assert(
      newEncodedState.toString ==
      """[
        |  [
        |    "world",
        |    [
        |      [
        |        [
        |          "2019-02-15T00:00:00Z",
        |          "2019-02-16T00:00:00Z"
        |        ],
        |        {
        |          "Todo" : {
        |            "backfill" : "98396503-b437-4b0b-96d5-2a4f87281c47"
        |          }
        |        }
        |      ],
        |      [
        |        [
        |          "2019-02-16T00:00:00Z",
        |          ">"
        |        ],
        |        {
        |          "Todo" : {
        |            
        |          }
        |        }
        |      ]
        |    ]
        |  ],
        |  [
        |    "hello1",
        |    [
        |      [
        |        [
        |          "2019-02-15T00:00:00Z",
        |          "2019-02-16T00:00:00Z"
        |        ],
        |        {
        |          "Done" : {
        |            "v" : "456"
        |          }
        |        }
        |      ],
        |      [
        |        [
        |          "2019-02-16T00:00:00Z",
        |          "2019-02-22T12:00:00Z"
        |        ],
        |        {
        |          "Done" : {
        |            "v" : "123"
        |          }
        |        }
        |      ],
        |      [
        |        [
        |          "2019-02-22T12:00:00Z",
        |          ">"
        |        ],
        |        {
        |          "Todo" : {
        |            
        |          }
        |        }
        |      ]
        |    ]
        |  ],
        |  [
        |    "hello2",
        |    [
        |      [
        |        [
        |          "2019-02-15T00:00:00Z",
        |          "2019-02-15T05:00:00Z"
        |        ],
        |        {
        |          "Done" : {
        |            "v" : "456"
        |          }
        |        }
        |      ],
        |      [
        |        [
        |          "2019-02-16T00:00:00Z",
        |          "2019-02-16T16:00:00Z"
        |        ],
        |        {
        |          "Done" : {
        |            "v" : "456"
        |          }
        |        }
        |      ],
        |      [
        |        [
        |          "2019-02-22T12:00:00Z",
        |          ">"
        |        ],
        |        {
        |          "Todo" : {
        |            
        |          }
        |        }
        |      ]
        |    ]
        |  ],
        |  [
        |    "hello3",
        |    [
        |      [
        |        [
        |          "2019-02-15T00:00:00Z",
        |          "2019-02-15T11:00:00Z"
        |        ],
        |        {
        |          "Done" : {
        |            "v" : "456"
        |          }
        |        }
        |      ],
        |      [
        |        [
        |          "2019-02-16T00:00:00Z",
        |          "2019-02-16T16:00:00Z"
        |        ],
        |        {
        |          "Done" : {
        |            "v" : "456"
        |          }
        |        }
        |      ],
        |      [
        |        [
        |          "2019-02-22T12:00:00Z",
        |          ">"
        |        ],
        |        {
        |          "Todo" : {
        |            
        |          }
        |        }
        |      ]
        |    ]
        |  ]
        |]""".stripMargin
    )
  }

  test("decode new state") {
    val legacyDecodedState = Database.dbStateDecoder(legacyState)
    assert(legacyDecodedState.isDefined)
    checkState(legacyDecodedState.get)

    val newEncodedState: Json = Database.dbStateEncoder(legacyDecodedState.get)
    val newDecodedState = Database.dbStateDecoder(newEncodedState)
    assert(newDecodedState.isDefined)
    checkState(newDecodedState.get)

    assert(legacyDecodedState.get(world).toList == newDecodedState.get(world).toList)
    assert(legacyDecodedState.get(hello1).toList == newDecodedState.get(hello1).toList)
    assert(legacyDecodedState.get(hello2).toList == newDecodedState.get(hello2).toList)
    assert(legacyDecodedState.get(hello3).toList == newDecodedState.get(hello3).toList)
  }

  test("re-encode twice") {
    val legacyDecodedState = Database.dbStateDecoder(legacyState)
    assert(legacyDecodedState.isDefined)
    checkState(legacyDecodedState.get)

    val newEncodedState: Json = Database.dbStateEncoder(legacyDecodedState.get)
    val newDecodedState = Database.dbStateDecoder(newEncodedState)
    assert(newDecodedState.isDefined)
    checkState(newDecodedState.get)

    val newEncodedState2: Json = Database.dbStateEncoder(newDecodedState.get)

    assert(newEncodedState == newEncodedState2)
  }

}