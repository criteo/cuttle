package com.criteo.cuttle.events

import java.time.Instant

import com.criteo.cuttle.Auth.User
import io.circe.{Decoder, Encoder, Json}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}

sealed trait Event {
  def created: Instant
  def asJson: Json = Event.encoder.apply(this)
}

case class JobSuccessForced(created: Instant,
                            createdBy: User,
                            jobId: String,
                            intervalStart: Instant,
                            intervalEnd: Instant) extends Event

object Event {
  import io.circe.java8.time.{decodeInstant, encodeInstant}
  implicit val encoder: Encoder[Event] = deriveEncoder
  implicit val decoder: Decoder[Event] = deriveDecoder
}
