package com.spark.transform

import java.util.Properties

import com.dimafeng.testcontainers.{ForAllTestContainer, PostgreSQLContainer}
import com.spark.transform.readFromPostgres._
import org.scalatest._

case class Coffees(coffeeName: String, price: Double)

class readFromPostgresTest extends FunSuite with SparkSessionTestWrapper with Matchers with ForAllTestContainer {

  import spark.implicits._

  override val container = PostgreSQLContainer()

  test("using java sql driver manager") {

    import java.sql.DriverManager

    val jdbcUrl = container.jdbcUrl
    val user = container.username
    val password = container.password

    Class.forName("org.postgresql.Driver")
    val conn = DriverManager.getConnection(jdbcUrl, user, password)

    Seq(
      """CREATE TABLE testTable (testColumn VARCHAR(255))""",
      """INSERT INTO testTable (testColumn) VALUES ('abc'), ('def')"""
    ).foreach(sql => {
      val sqlStatement = conn.prepareStatement(sql)

      sqlStatement.executeUpdate()
      sqlStatement.close()
    })

    val props = new Properties()
    props.put("user", user)
    props.put("password", password)
    props.put("driver", "org.postgresql.Driver")

    val result = transform(spark, jdbcUrl, "testTable", props)
    val expected = Seq("abc", "def").toDF("testColumn")

    result.collect should contain theSameElementsAs expected.collect
  }

  test("using Slick") {

    import slick.jdbc.PostgresProfile.api._
    import slick.lifted.Tag

    import scala.concurrent._
    import scala.concurrent.duration._

    class CoffeesTable(tag: Tag) extends Table[Coffees](tag, "coffees") {
      def coffeeName = column[String]("coffeeName")
      def price = column[Double]("price")
      def * = (coffeeName, price).mapTo[Coffees]
    }

    val coffees = TableQuery[CoffeesTable]

    val jdbcUrl = container.jdbcUrl
    val user = container.username
    val password = container.password

    val db = Database.forURL(jdbcUrl, user, password)

    val rowsToInsert = Seq(
      Coffees("Colombian", 7.99),
      Coffees("French_Roast", 8.99),
      Coffees("Espresso", 9.99)
    )

    val actions = {
      coffees.schema.create >>
      (coffees ++= rowsToInsert)
    }

    Await.result(
      db.run(actions),
      Duration.Inf
    )

    Class.forName("org.postgresql.Driver")

    val props = new Properties()
    props.put("user", user)
    props.put("password", password)
    props.put("driver", "org.postgresql.Driver")

    val result = transform(spark, jdbcUrl, "COFFEES", props)
    val expected = rowsToInsert.toDF

    result.collect should contain theSameElementsAs expected.collect
  }

  //  test("run random sql queries") {
  //    val jdbcUrl = container.jdbcUrl
  //    val user = container.username
  //    val password = container.password
  //
  //    val db = Database.forURL(jdbcUrl, user, password)
  //
  //    // doesn't work
  //    db.run(sqlu"ALTER TABLE public.COFFEES OWNER TO postgres")
  //  }
}
