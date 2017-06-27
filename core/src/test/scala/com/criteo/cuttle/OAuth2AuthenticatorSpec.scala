package com.criteo.cuttle

import com.criteo.cuttle.SuiteUtils.{signToken, testAppSecret}
import com.criteo.cuttle.authentication.{OAuth2Authenticator, OAuth2ClientConfig}
import lol.http._
import org.scalatest.FunSuite

/**
  * Created by a.dufranne on 27/06/17.
  */
class OAuth2AuthenticatorSpec extends FunSuite {

  test("OAuth2Auth should not find user if no jwt cookie") {
    val request = getFakeRequest()

    val actual = getOAuth2Auth().getAuthenticatedUser(request)
    assert(actual == None)
  }

  test("OAuth2Auth.getAuthenticatedUser should authenticate user if valid jwt cookie") {
    val token = signToken(Map("uid" -> "testuid"), testAppSecret)
    val request = getFakeRequest().addHeaders(h"cookie" -> HttpString(s"jwt=$token"))

    val actual = getOAuth2Auth().getAuthenticatedUser(request)
    assert(actual.isDefined)
  }

  test("OAuth2Auth.getAuthenticatedUser should return None without failing if invalid jwt cookie") {
    val token = "invalidtoken"
    val request = getFakeRequest().addHeaders(h"cookie" -> HttpString(s"jwt=$token"))

    val actual = getOAuth2Auth().getAuthenticatedUser(request)
    assert(actual == None)
  }

  def getOAuth2Auth() =
    new OAuth2Authenticator(
      OAuth2ClientConfig(
        redirectURL = "redirectURL",
        appSecret = testAppSecret,
        authorizeURL = "authorizeURL",
        clientId = "myclientid",
        responseType = "token",
        realm = "cuttle_users"
      ))

  /**
    * GET request with no header.
    */
  def getFakeRequest(url: String = "") =
    Request(
      GET,
      url,
      "http",
      Content.empty,
      Map.empty[HttpString, HttpString]
    )
}
