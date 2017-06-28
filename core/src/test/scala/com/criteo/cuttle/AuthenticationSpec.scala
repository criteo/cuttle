package com.criteo.cuttle

import com.criteo.cuttle.authentication._
import com.criteo.cuttle.authentication.authentication.AuthenticatedService
import org.scalatest.FunSuite
import lol.http._

class AuthenticationSpec extends FunSuite {

  import SuiteUtils._

  test("GuestAuth should return Guest user") {
    val actual = GuestAuth.authenticate(getFakeRequest())
    assert(actual == Right(User("Guest")))
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
    assertHttpBasicUnAuthorized(actual)
  }

  test("HttpAuth should answer 401 without Authorization header") {
    val request = getFakeRequest()

    val actual = getBasicAuth().authenticate(request)
    assertHttpBasicUnAuthorized(actual)
  }

  test("HttpAuth should answer 401 when invalid base64 string") {
    val request = getFakeRequest()
      .addHeaders(h"Authorization" -> h"Basic àààààààààà")

    val actual = getBasicAuth().authenticate(request)
    assertHttpBasicUnAuthorized(actual)
  }

  test("HttpAuth should answer user with valid basic http header") {
    val request = getFakeRequest()
      .addHeaders(h"Authorization" -> h"Basic bG9naW46cGFzc3dvcmQK")

    val actual = getBasicAuth().authenticate(request)
    assert(actual.isRight)
  }

  test("HttpAuth should answer user with valid basic http header when many spaces") {
    val request = getFakeRequest()
      .addHeaders(h"Authorization" -> h"Basic   bG9naW46cGFzc3dvcmQK")

    val actual = getBasicAuth().authenticate(request)
    assert(actual.isRight)
  }

  test("HttpAuth should deny access to unauthorized user") {
    // "login:wrongpassword"
    val request = getFakeRequest()
      .addHeaders(h"Authorization" -> h"Basic bG9naW46d3JvbmdwYXNzd29yZAo")

    val actual = getBasicAuth().authenticate(request)
    assert(actual.isLeft)
  }

  test("Authenticator should allow only part of the api to be authenticated") {
    val publicApi: PartialService = {
      case GET at "/public" => Ok("public")
    }
    val privateAuthenticatedApi: AuthenticatedService = {
      case GET at "/private/authenticated" =>
        _ =>
          Ok("privateauthenticated")
    }

    val privateNonAuthenticatedApi: AuthenticatedService = {
      case GET at "/private/nonauthenticated" =>
        _ =>
          Ok("privatenonauthenticated")
    }

    val completeApi = publicApi
      .orElse(NoAuth(privateNonAuthenticatedApi))
      .orElse(YesAuth(privateAuthenticatedApi))
      .orElse(defaultWith404)

    assertOk(completeApi, "/public")
    assertUnAuthorized(completeApi, "/private/nonauthenticated")
    assertOk(completeApi, "/private/authenticated")
    assertNotFound(completeApi, "/nonexistingurl")
  }

  val defaultWith404 : PartialService = {
    case _ => Response(404)
  }

  def assertCodeAtUrl(code: Int)(api: Service)(url: String): Unit =
    assert(api(getFakeRequest(url)).value.get.get.status == code, url)

  def assertOk: (Service, String) => Unit = assertCodeAtUrl(200)(_)(_)

  def assertUnAuthorized: (Service, String) => Unit = assertCodeAtUrl(401)(_)(_)

  def assertNotFound: (Service, String) => Unit = assertCodeAtUrl(404)(_)(_)

  def assertHttpBasicUnAuthorized(authResponse: Either[Response, User]): Unit =
    assert(authResponse match {
      case Left(r) => {
        val maybeRealm = r.headers.get(h"WWW-Authenticate")
        r.status == 401 && maybeRealm == Some(s"""Basic Realm="${testRealm}"""")
      }
      case _ => false
    })
}

object SuiteUtils {

  val testRealm = "myfakerealm"
  val testAppSecret = "myappsecret"
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

  /** *
    * Gets a BasicAuth allowing
    * only login:password
    */
  def getBasicAuth() = basicAuth

  private def isAuthorized(t: (String, String)): Boolean = {
    t._1 == "login" && t._2 == "password"
  }

  private val basicAuth = BasicAuth(isAuthorized, testRealm)
}

object YesAuth extends Authenticator {
  override def authenticate(r: Request): Either[Response, User] = Right(User("yesuser"))
}

object NoAuth extends Authenticator {
  override def authenticate(r: Request): Either[Response, User] = Left(Response(401))
}
