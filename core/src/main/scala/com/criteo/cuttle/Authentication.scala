package com.criteo.cuttle

import java.util.Base64

import lol.http._

import scala.util.Try

/**
  * Trait describing authentication methods
  */
trait Authenticator {
  /**
    *
    * @param wrappedAuthedService
    * @return
    */
  def apply(wrappedAuthedService: (User) => Service): Service = (request: Request) => {
    authenticate(request)
      .fold(user => wrappedAuthedService(user)(request), identity)
  }

  /**
    * Performs authentication on request.
    * @param r the request to be authenticated
    * @return either an authenticated user or a response.
    */
  def authenticate(r: Request): Either[User, Response]
}

case class User(userName: String)

case object NoAuth extends Authenticator {
  /**
    * Authenticated any user as Guest.
    * @param r request to be authenticated
    * @return Authenticated user
    */
  override def authenticate(r: Request): Either[User, Response] = Left(User("Guest"))
}

/**
  * Implementation of the HTTP Basic auth.
  * @param credentialsValidator method to validate credentials.
  */
case class BasicAuth(credentialsValidator : ((String,String)) => Boolean) extends Authenticator {

  val scheme = "Basic"
  val unauthorizedResponse = Response(401).addHeaders(h"WWW-Authenticate" -> h"""Basic realm="User Visible Realm"""")

  /**
    * HTTP Basic auth implementation.
    * @param r request to be authenticated
    * @return either an authenticated user or an unauthorized response
    */
  override def authenticate(r: Request): Either[User, Response] =
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
        case (l,p)  if credentialsValidator((l,p)) => User(l)
      })
      .toLeft(unauthorizedResponse)
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
