package com.criteo.cuttle.events

import java.time.Instant

import com.criteo.cuttle.Auth.User
import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.java8.time._

sealed trait Event {
  def created: Instant
}

case class JobSuccessForced(created: Instant,
                            createdBy: User,
                            jobId: String,
                            intervalStart: Instant,
                            intervalEnd: Instant) extends Event

object JobSuccessForced {
  implicit val encoder: Encoder[JobSuccessForced] = deriveEncoder
  implicit val decoder: Decoder[JobSuccessForced] = deriveDecoder
}

object Event {
  implicit val encoder: Encoder[Event] = deriveEncoder
  implicit val decoder: Decoder[Event] = deriveDecoder
}
