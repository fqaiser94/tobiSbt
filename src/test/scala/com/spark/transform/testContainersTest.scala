package com.spark.transform

import java.sql.DriverManager
import java.util.Properties

import com.dimafeng.testcontainers.{ForAllTestContainer, PostgreSQLContainer}
import com.spark.transform.readFromPostgres._
import org.scalatest._

class PostgresqlSpec extends FlatSpec with ForAllTestContainer with SparkSessionTestWrapper {

  override val container = PostgreSQLContainer()

  "PostgreSQL container" should "be started" in {
    val conn = DriverManager.getConnection(container.jdbcUrl, container.username, container.password)

    val prepare_statement = conn.prepareStatement("CREATE TABLE testTable (testColumn VARCHAR(255))")
    prepare_statement.executeUpdate()
    prepare_statement.close()

    val props = new Properties()
    props.put("user", container.username)
    props.put("password", container.password)
    props.put("driver", "org.postgresql.Driver")

    val result = transform(spark, container.jdbcUrl, "testTable", props)

    result.show
  }

}