package com.criteo.cuttle.authentication

import com.auth0.jwt.{JWTSigner, JWTVerifier}

import scala.collection.JavaConverters._
import scala.util.Try

trait JWT {
  type Claims = Map[String, Object]

  def signToken(claims: Claims, secret: String): String = new JWTSigner(secret).sign(claims.asJava)

  def decodeToken(token: String, secret: String): Try[Claims] = Try {
    new JWTVerifier(secret).verify(token).asScala.toMap
  }
}
