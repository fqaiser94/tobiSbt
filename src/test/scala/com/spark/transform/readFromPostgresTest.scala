package com.spark.transform

import com.spark.transform.readFromPostgres._
import com.spark.transform.SlickPGUtils._
import java.util.Properties

import com.spotify.docker.client.DefaultDockerClient
import com.whisk.docker.impl.spotify.SpotifyDockerFactory
import com.whisk.docker.{DockerContainer, DockerFactory, DockerReadyChecker, HostConfig}
import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.concurrent.{PatienceConfiguration, ScalaFutures}
import org.scalatest.time.{Second, Seconds, Span}

import scala.concurrent._
import scala.concurrent.duration._
import scala.util.{Failure, Success}
import org.scalatest._


class readFromPostgresTest extends FlatSpec with SparkSessionTestWrapper with Matchers with BeforeAndAfterAll with GivenWhenThen with ScalaFutures with DockerPostgresService {

  import spark.implicits._

  implicit val pc = PatienceConfig(Span(1000, Seconds), Span(1, Second))


  def neo4jPort: Int = postgresContainer.getPorts().futureValue.apply(PostgresAdvertisedPort)

  // isContainerReady(postgresContainer).value shouldBe true

  //  isContainerReady(postgresContainer).onComplete {
  //    case Success(_) => println("container ready")
  //    case Failure(_) => println("An error has occurred")
  //  }

  //test("testTransform") {

  "all containers" should "be ready at the same time" in {
    dockerContainers.map(_.image).foreach(println)
    dockerContainers.forall(_.isReady().futureValue) shouldBe true
  }

  "An empty Set" should "have size 0" in {

    //    DB.withConnection { implicit c =>
    //      SQL("INSERT INTO userTable values({userId},{userName})").on("userId" -> "User's id", "userName" -> "your_name").executeInsert();
    //    }

    val props = new Properties()
    props.put("user", PostgresUser)
    props.put("password", PostgresPassword)
    props.put("driver", "org.postgresql.Driver")

    val database = "testDb"
    val dbUrl = s"jdbc:postgresql://localhost:$PostgresAdvertisedPort/$database?autoReconnect=true&useSSL=false"

    val result = transform(spark, dbUrl, "testTable", props)

    println("farooq")

    //isContainerReady(postgresContainer).onComplete {
    postgresContainer.isReady.onComplete({
      case Success(_) => {
        println("container ready")

        val result = transform(spark, dbUrl, "testTable", props)
        result.show
      }
      case Failure(_) => {
        println("container never became ready")
      }
    })
  }

  // dropDb()

}


//class AllAtOnceSpec extends FlatSpec with Matchers with BeforeAndAfterAll with GivenWhenThen with ScalaFutures with DockerElasticsearchService {
//
//  override implicit val dockerFactory: DockerFactory = new SpotifyDockerFactory(DefaultDockerClient.fromEnv().build())
//
//  implicit val pc = PatienceConfig(Span(300, Seconds), Span(1, Second))
//
//  "all containers" should "be ready at the same time" in {
//    dockerContainers.map(_.image).foreach(println)
//    dockerContainers.forall(_.isReady().futureValue) shouldBe true
//  }
//}
