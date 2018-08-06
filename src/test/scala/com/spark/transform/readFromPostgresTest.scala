package com.spark.transform

import com.spark.schema.merakiDb.{CoffeesSchema, CoffeesTable}
import com.spark.transform.readFromPostgres._

class readFromPostgresTest extends SparkTest with PostgresTest {

  import spark.implicits._

  test("using java sql driver manager") {

    import java.sql.DriverManager

    val jdbcUrl = container.jdbcUrl
    val user = container.username
    val password = container.password

    val connection = DriverManager.getConnection(jdbcUrl, user, password)

    Seq(
      """CREATE TABLE COFFEES (coffeeName VARCHAR(255), price FLOAT)""",
      """INSERT INTO COFFEES (coffeeName, price) VALUES ('Colombian', 7.99), ('French_Roast', 8.99), ('Espresso', 9.99)"""
    ).foreach(sql => {
      val sqlStatement = connection.prepareStatement(sql)

      sqlStatement.executeUpdate()
      sqlStatement.close()
    })

    val props = genProps(user, password, container.driverClassName)

    val result = transform(spark, jdbcUrl, "coffees", props)
    val expected = Seq(
      CoffeesSchema("Colombian", 7.99),
      CoffeesSchema("French_Roast", 8.99),
      CoffeesSchema("Espresso", 9.99)
    ).toDF("coffeeName", "price")

    result.collect should contain theSameElementsAs expected.collect
  }

  test("using Slick") {

    import slick.jdbc.PostgresProfile.api._

    import scala.concurrent._
    import scala.concurrent.duration._

    val coffees = TableQuery[CoffeesTable]

    val jdbcUrl = container.jdbcUrl
    val user = container.username
    val password = container.password

    val db = Database.forURL(jdbcUrl, user, password)

    val rowsToInsert = Seq(
      CoffeesSchema("Colombian", 7.99),
      CoffeesSchema("French_Roast", 8.99),
      CoffeesSchema("Espresso", 9.99)
    )

    val actions = DBIO.seq(
        coffees.schema.create,
        coffees ++= rowsToInsert
    )

    Await.result(
      db.run(actions),
      Duration.Inf
    )

    val props = genProps(user, password, container.driverClassName)

    val result = transform(spark, jdbcUrl, "coffees", props)
    val expected = rowsToInsert.toDF

    result.collect should contain theSameElementsAs expected.collect
  }
}
