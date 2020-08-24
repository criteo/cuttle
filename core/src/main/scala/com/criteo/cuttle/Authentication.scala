package com.criteo.cuttle

import cats.effect.IO

import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}

import org.http4s.server.AuthMiddleware
import cats.data.{Kleisli, OptionT}

/**
  * The cuttle API is private for any write operation while it is publicly
  * open for any read only operation. It allows to make it easy to build tooling
  * that monitor any running cuttle scheduler while restricting access to potentially
  * dangerous operations.
  *
  * The UI access itself requires authentication.
  */
object Auth {

  case class User(userId: String)

  object User {
    implicit val encoder: Encoder[User] = deriveEncoder
    implicit val decoder: Decoder[User] = deriveDecoder
  }

  val GuestAuth: AuthMiddleware[IO, User] = AuthMiddleware(Kleisli { _ =>
    OptionT.some(User("guest"))
  })

}
