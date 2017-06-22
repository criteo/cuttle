package com.criteo.cuttle

import org.scalatest.FunSuite
import lol.http._

class AuthenticationSpec extends FunSuite {
  import SuiteUtils._

  test("NoAuth should return Guest user") {
    val actual = NoAuth.authenticate(getFakeRequest())
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
    assertUnauthorized(actual)
  }

  test("HttpAuth should answer 401 without Authorization header") {
    val request = getFakeRequest()

    val actual = getBasicAuth().authenticate(request)
    assertUnauthorized(actual)
  }

  test("HttpAuth should answer 401 when invalid base64 string") {
    val request = getFakeRequest()
      .addHeaders(h"Authorization" -> h"Basic àààààààààà")

    val actual = getBasicAuth().authenticate(request)
    assertUnauthorized(actual)
  }

  test("HttpAuth should answer user with valid basic http header") {
    val request = getFakeRequest()
      .addHeaders(h"Authorization" -> h"Basic bG9naW46cGFzc3dvcmQK")

    val actual = getBasicAuth().authenticate(request)
    assert(actual.isRight)
  }

  test("HttpAuth should answer user with valid basic http header when mnay spaces") {
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

  test("Public service should be chainable with secure service") {
    import com.criteo.cuttle.utils.PartialServiceConverter

    val publicService : PartialService = {
      case GET at "/api/public" => Ok("")
    }

    def secureService(user : User): PartialService = {
      case GET at "/api/private" => Ok(s"hi user ${user.userName}")
      case _ => NotFound("")
    }

    // mimic an api always authenticated
    // with a Guest user
    val completeApi = publicService
        .orFinally(NoAuth(secureService))

    // mimic an api non authenticated
    val completeUnauthenticatedApi = publicService
        .orFinally(ZeroAuth(secureService))

    assert(
      completeApi(getFakeRequest("/api/private")).value.get.get.status == 200,
      "access to private api should be allowed when authenticated"
    )
    assert(
      completeApi(getFakeRequest("/api/public")).value.get.get.status == 200,
      "access to public api should be granted when authenticated"
    )

    assert(
      completeUnauthenticatedApi(getFakeRequest("/api/private")).value.get.get.status == 401,
      "access to private api should be denied")
    assert(completeUnauthenticatedApi(
      getFakeRequest("/api/public")).value.get.get.status == 200,
      "access to public api should be granted even when non authenticated"
    )

    // problem here,
    // with this solution we get only 401 as long
    // as there is a secure api.
    assert(
      completeUnauthenticatedApi(getFakeRequest("/api/nonexisting")).value.get.get.status == 404
    )
  }

  def assertUnauthorized(authResponse : Either[Response, User]) : Unit  = {
    assert(authResponse match {
      case Left(r) => {
        val maybeRealm = r.headers.get(h"WWW-Authenticate")
        r.status == 401 && maybeRealm == Some(s"""Basic Realm="${testRealm}"""")
      }
      case _        => false
    })
  }
}

object SuiteUtils {

  val testRealm = "myfakerealm"

  /**
    * GET request with no header.
    */
  def getFakeRequest(url : String = "") = Request(
    GET,
    url,
    "http",
    Content.empty,
    Map.empty[HttpString, HttpString]
  )

  /***
    * Gets a BasicAuth allowing
    * only login:password
    */
  def getBasicAuth() = basicAuth

  private def isAuthorized(t : (String,String)) : Boolean = {
    t._1 == "login" && t._2 == "password"
  }

  private val basicAuth = BasicAuth(isAuthorized, testRealm)

  object ZeroAuth extends Authenticator {
    override def authenticate(r: Request): Either[Response, User] = Left(Response(401))
  }
}
