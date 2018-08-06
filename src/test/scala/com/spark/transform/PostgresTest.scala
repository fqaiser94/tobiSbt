package com.spark.transform

import com.dimafeng.testcontainers.{ForEachTestContainer, PostgreSQLContainer}
import org.scalatest._

trait PostgresTest extends FunSuite with ForEachTestContainer {
  override val container = PostgreSQLContainer()
}