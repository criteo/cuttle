package com.criteo.cuttle

import java.util.Base64

import cats.effect.IO
import lol.http._
import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}

import scala.util.Try

/**
  * The cuttle API is private for any write operation while it is publicly
  * open for any read only operation. It allows to make it easy to build tooling
  * that monitor any running cuttle scheduler while restricting access to potentially
  * dangerous operations.
  *
  * The UI access itself requires authentication.
  */
object Auth {

  /**
    * An [[Authenticator]] takes care of extracting the User from an HTTP request.
    */
  trait Authenticator {

    private[cuttle] def apply(s: AuthenticatedService): PartialService =
      new PartialFunction[Request, IO[Response]] {
        override def isDefinedAt(request: Request): Boolean =
          s.isDefinedAt(request)

        override def apply(request: Request): IO[Response] =
          authenticate(request)
            .fold(IO.pure, user => {
              // pass through when authenticated
              s(request)(user)
            })
      }

    /**
      * Authenticate an HTTP request.
      *
      * @param request the HTTP request to be authenticated.
      * @return either an authenticated user or an error response.
      */
    def authenticate(request: Request): Either[Response, User]
  }

  /**
    * A connected [[User]].
    */
  case class User(userId: String)

  object User {
    implicit val encoder: Encoder[User] = deriveEncoder
    implicit val decoder: Decoder[User] = deriveDecoder
  }

  /**
    * Default implementation of Authenticator that authenticate any request
    * as Guest. It basically disables the authentication.
    */
  case object GuestAuth extends Authenticator {
    override def authenticate(r: Request): Either[Response, User] = Right(User("Guest"))
  }

  /**
    * Implementation of [[Authenticator]] that rely on HTTP Basic auth.
    *
    * @param credentialsValidator validate the (user,password) credentials.
    * @param userVisibleRealm The user visible realm.
    */
  case class BasicAuth(
    credentialsValidator: ((String, String)) => Boolean,
    userVisibleRealm: String = "cuttle_users"
  ) extends Authenticator {

    val scheme = "Basic"
    val unauthorizedResponse =
      Response(401).addHeaders(h"WWW-Authenticate" -> HttpString(s"""Basic realm="${userVisibleRealm}""""))

    /**
      * HTTP Basic auth implementation.
      * @param r request to be authenticated
      * @return either an authenticated user or an unauthorized response
      */
    override def authenticate(r: Request): Either[Response, User] =
      r.headers
        .get(h"Authorization")
        .flatMap({
          case s if s.toString().startsWith(scheme) => {
            val base64credentials = s.toString().drop(scheme.size).trim()
            BasicAuth.decodeBase64Credentials(base64credentials)
          }
          case _ => None
        })
        .collect({
          case (l, p) if credentialsValidator((l, p)) => User(l)
        })
        .toRight(unauthorizedResponse)
  }

  private[cuttle] object BasicAuth {
    def decodeBase64Credentials(credentials: String): Option[(String, String)] =
      Try(Base64.getDecoder.decode(credentials)).toOption
        .flatMap((decoded: Array[Byte]) => {
          val splitted = new String(decoded, "utf-8").trim().split(":", 2)
          if (splitted.size == 1) {
            None
          } else {
            Some((splitted(0) -> splitted(1)))
          }
        })
  }

  /** A [[lol.http.PartialService PartialService]] that requires an authenticated
    * user in request handler's scope. */
  type AuthenticatedService = PartialFunction[Request, (User => IO[Response])]

  private[cuttle] def defaultWith(response: IO[Response]): PartialService = {
    case _ => response
  }

}
