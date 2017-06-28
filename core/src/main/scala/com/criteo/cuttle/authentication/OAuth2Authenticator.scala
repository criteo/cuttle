package com.criteo.cuttle.authentication

import java.net.URL

import lol.http.{HttpString, Request, Response}

case class OAuth2ClientConfig(
                               val redirectURL: String,
                               val appSecret: String,
                               val authorizeURL: String,
                               val clientId: String,
                               val responseType: String,
                               val realm: String
                             )

/**
  * Describes the information we extract from JWT tokens
  */
case class TokenInfo(uid: String)

class OAuth2Authenticator(config: OAuth2ClientConfig) extends Authenticator with JWT {
  override def authenticate(r: Request): Either[Response, User] =
    getAuthenticatedUser(r).toRight(Response(401))

  def getAuthenticatedUser(request: Request): Option[User] = {
    val cookies: Map[String, String] = RequestUtils.getCookies(request)

    if (bypass(request.path)) {
      Some(User("Guest"))
    } else {
      for {
        jwt <- cookies.get("jwt")
        token <- verifyToken(jwt)
        user = User(token.uid)
      } yield user
    }
  }

  def verifyToken(token: String): Option[TokenInfo] =
    for {
      claims <- decodeToken(token, config.appSecret).toOption
      uid <- claims.get("uid")
    } yield TokenInfo(uid.asInstanceOf[String])

  def bypass(path: String): Boolean = path.startsWith("/login")

  def getLoginURL(): URL = new URL(
    config.authorizeURL +
      s"?client_id=${config.clientId}" +
      s"&redirect_uri=${config.redirectURL.toString}" +
      s"&response_type=${config.responseType}" +
      s"&realm=${config.realm}" +
      s"&scope=openid"
  )
}

object RequestUtils {

  /**
    * Retrieves cookies from request
    *
    * @param request the request to parse.
    * @return a Map with cookieName as key,
    *         and cookieValue as cookie.
    */
  def getCookies(request: Request): Map[String, String] =
    request.headers
      .get(HttpString("cookie"))
      .map(x => x.toString())
      .map(cookieString => {
        cookieString
          .stripMargin(';')
          .split(';')
          .map((keyValuePair) => {
            val splitted = keyValuePair.split("=", 2)
            val cookieValue = if (splitted.size == 1) {
              ""
            } else {
              splitted(1).trim()
            }

            splitted(0).trim() -> cookieValue
          })
          .toMap
      }) getOrElse (Map.empty[String, String])
}
