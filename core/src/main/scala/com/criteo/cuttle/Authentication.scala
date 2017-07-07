package com.criteo.cuttle

import java.util.Base64

import com.criteo.cuttle.authentication._
import lol.http._

import scala.concurrent.Future
import scala.util.Try

trait Authenticator {

  /**
    * Turns an AuthenticatedService into a PartialService
    * with embedded authentication
    * Will try to authenticate *only* requests in the domain
    * of the AuthenticatedService, other requests are passed
    * through.
    * @param s the service to wrap.
    * @return a partial service.
    */
  def apply(s: AuthenticatedService): PartialService =
    new PartialFunction[Request, Future[Response]] {
      override def isDefinedAt(request: Request): Boolean =
        s.isDefinedAt(request)

      override def apply(request: Request): Future[Response] =
        authenticate(request)
          .fold(identity, user => {
            // pass through when authenticated
            s(request)(user)
          })
    }

  /**
    * Performs authentication on request.
    * @param r the request to be authenticated
    * @return either an authenticated user or a response.
    */
  def authenticate(r: Request): Either[Response, User]
}

case class User(userId: String)

/**
  * Authenticates every request with a Guest user
  */
case object GuestAuth extends Authenticator {

  override def authenticate(r: Request): Either[Response, User] = Right(User("Guest"))
}

/**
  * Implementation of the HTTP Basic auth.
  * @param credentialsValidator function to validate credentials.
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

object BasicAuth {

  /**
    * Decode base64 encoded credentials for http basic auth
    * ie "login:password" to base64.
    * @param credentials the base64 encoded credentials
    * @return Some(credentials) or None if parsing failed
    */
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

package object authentication {

  /**
    * PartialService with authenticated user in
    * request handler's scope.
    */
  type AuthenticatedService = PartialFunction[Request, (User => Future[Response])]

  def defaultWith(response: Future[Response]): PartialService = {
    case _ => response
  }
}
