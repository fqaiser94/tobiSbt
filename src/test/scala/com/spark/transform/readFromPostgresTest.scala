package com.spark.transform

import java.sql.DriverManager
import java.util.Properties

import com.spark.transform.readFromPostgres._
import com.dimafeng.testcontainers.{ForAllTestContainer, PostgreSQLContainer}
import org.scalatest._

//import slick.jdbc.H2Profile.api._
//import scala.concurrent.ExecutionContext.Implicits.global
//
//import slick.dbio.Effect.{Read, Schema, Write}
//
//import scala.concurrent._
//import scala.concurrent.duration._
//import slick.jdbc.PostgresProfile.api._
//import slick.lifted.{ProvenShape, TableQuery}
//import slick.sql.{FixedSqlAction, FixedSqlStreamingAction}


case class Coffee(name: String, price: Double)

class readFromPostgresTest extends FunSuite with SparkSessionTestWrapper with Matchers with ForAllTestContainer {

  import spark.implicits._

  override val container = PostgreSQLContainer()

  test("PostgreSQL container be started") {
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

//  test("using Slick API") {
//    val insertActions = DBIO.seq(
//      coffees += ("Colombian", 101, 7.99, 0, 0),
//
//      coffees ++= Seq(
//        ("French_Roast", 49, 8.99, 0, 0),
//        ("Espresso", 150, 9.99, 0, 0)
//      ),
//
//      // "sales" and "total" will use the default value 0:
//      coffees.map(c => (c.name, c.supID, c.price)) += ("Colombian_Decaf", 101, 8.99)
//    )
//
//    // Get the statement without having to specify a value to insert:
//    val sql = coffees.insertStatement
//
//    // compiles to SQL:
//    //   INSERT INTO "COFFEES" ("COF_NAME","SUP_ID","PRICE","SALES","TOTAL") VALUES (?,?,?,?,?)
//
//  }
}


//object Example01 extends App {
//
//  case class Album(artist: String, title: String, year: Int, id: Long = 0)
//
//  // A standard Slick table type representing an SQL table type to store instances
//  // of type Album.
//  class AlbumTable(tag: Tag) extends Table[Album](tag, "albums") {
//
//    // definitions of each of the columns
//    def artist: Rep[String] = column[String]("artist")
//    def title: Rep[String] = column[String]("title")
//    def year: Rep[Int] = column[Int]("year")
//
//    // the 'id' column has a couple of extra 'flags' to say
//    // 'make this a primary key' and 'make this an auto incrementing primary key'
//    def id: Rep[Long] = column[Long]("id", O.PrimaryKey, O.AutoInc)
//
//    // this is the default projection for the table. It tells us how to convert between a
//    // tuple of these columns of the database and the Album datatype that we want to map
//    // using this table.
//    def * : ProvenShape[Album] = (artist, title, year, id) <> (Album.tupled, Album.unapply)
//  }
//
//  lazy val AlbumTable = TableQuery[AlbumTable]
//}