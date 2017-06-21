package com.criteo.cuttle

import org.scalatest.FunSuite
import lol.http._

class AuthenticationSpec extends FunSuite {
  import SuiteUtils._

  test("NoAuth should return Guest user") {
    val actual = NoAuth.authenticate(getFakeRequest())
    assert(actual == Left(User("Guest")))
  }

  test("HttpAuth.decodeBase64Credentials should match valid credentials") {
    val inputCreds = "bG9naW46cGFzc3dvcmQK"
    val expected = ("login", "password")

    val actual = BasicAuth.decodeBase64Credentials(inputCreds)
    assert(actual.isDefined && actual.get == expected)
  }

  test("HttpAuth should answer 401 with wrong Authorization header") {
    val request = getFakeRequest()
      .addHeaders(HttpString("Authorization") -> HttpString("NotBasic"))

    val actual = getBasicAuth().authenticate(request)
    assert(actual match {
      case Right(r) => r.status == 401 && r.headers.get(h"WWW-Authenticate") == Some(h"""Basic realm="User Visible Realm"""")
      case _        => false
    })
  }

  test("HttpAuth should answer 401 without Authorization header") {
    val request = getFakeRequest()

    val actual = getBasicAuth().authenticate(request)
    assert(actual match {
      case Right(r) => r.status == 401 && r.headers.get(h"WWW-Authenticate") == Some(h"""Basic realm="User Visible Realm"""")
      case _        => false
    })
  }

  test("HttpAuth should answer 401 when invalid base64 string") {
    val request = getFakeRequest()
      .addHeaders(h"Authorization" -> h"Basic àààààààààà")

    val actual = getBasicAuth().authenticate(request)
    assert(actual match {
      case Right(r) => r.status == 401 && r.headers.get(h"WWW-Authenticate") == Some(h"""Basic realm="User Visible Realm"""")
      case _        => false
    })
  }

  test("HttpAuth should answer user with valid basic http header") {
    val request = getFakeRequest()
      .addHeaders(h"Authorization" -> h"Basic bG9naW46cGFzc3dvcmQK")

    val actual = getBasicAuth().authenticate(request)
    assert(actual.isLeft)
  }

  test("HttpAuth should answer user with valid basic http header when mnay spaces") {
    val request = getFakeRequest()
      .addHeaders(h"Authorization" -> h"Basic   bG9naW46cGFzc3dvcmQK")

    val actual = getBasicAuth().authenticate(request)
    assert(actual.isLeft)
  }

  test("HttpAuth should deny access to unauthorized user") {
    // "login:wrongpassword"
    val request = getFakeRequest()
      .addHeaders(h"Authorization" -> h"Basic bG9naW46d3JvbmdwYXNzd29yZAo")

    val actual = getBasicAuth().authenticate(request)
    assert(actual.isRight)
  }
}

object SuiteUtils {
  /**
    * GET request with no header.
    */
  def getFakeRequest() = fakeRequest

  /***
    * Gets a BasicAuth allowing
    * only login:password
    */
  def getBasicAuth() = basicAuth

  def isAuthorized(t : (String,String)) : Boolean = {
    t._1 == "login" && t._2 == "password"
  }

  val basicAuth = BasicAuth(isAuthorized)

  val fakeRequest = Request(
    GET,
    "",
    "http",
    Content.empty,
    Map.empty[HttpString, HttpString]
  )
}
