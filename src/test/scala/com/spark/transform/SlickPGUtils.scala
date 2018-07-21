package com.spark.transform

import scala.concurrent.Await
import scala.concurrent.duration._
import org.postgresql.util.PSQLException
import slick.driver.PostgresDriver
import slick.driver.PostgresDriver.api._

import scala.util.Try

object SlickPGUtils {

  private val actionTimeout = 10 second
  private val driver = "org.postgresql.Driver"

  def createDb(host: String, port: Int, dbName: String, user: String, pwd: String) = {
    val onlyHostNoDbUrl = s"jdbc:postgresql://$host:$port/"
    using(Database.forURL(onlyHostNoDbUrl, user = user, password = pwd, driver = driver)) { conn =>
      Await.result(conn.run(sqlu"CREATE DATABASE #$dbName"), actionTimeout)
    }
  }

  def dropDb(host: String, port: Int, dbName: String, user: String, pwd: String) = {
    val onlyHostNoDbUrl = s"jdbc:postgresql://$host:$port/"
    try {
      using(Database.forURL(onlyHostNoDbUrl, user = user, password = pwd, driver = driver)) { conn =>
        Await.result(conn.run(sqlu"DROP DATABASE #$dbName"), actionTimeout)
      }
    } catch {
      // ignore failure due to db not exist
      case e:PSQLException => if (e.getMessage.equals(s""""database "$dbName" does not exist""")) {/* do nothing */}
      case e:Throwable => throw e // escalate other exceptions
    }
  }

  private def using[A <: {def close() : Unit}, B](resource: A)(f: A => B): B =
    try {
      f(resource)
    } finally {
      Try {
        resource.close()
      }.failed.foreach(err => throw new Exception(s"failed to close $resource", err))
    }
}
