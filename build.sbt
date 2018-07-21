name := "spark"

version := "0.1"

// need to specify all 3 parts of scalaVersion
scalaVersion := "2.11.12"

// automatically appends to scalaVersion to 2nd part
libraryDependencies += "org.specs2" %% "specs2-core" % "4.3.2" % Test

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.1"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.3.1"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % Test

libraryDependencies += "com.whisk" %% "docker-testkit-scalatest" % "0.9.5" % Test

libraryDependencies +=  "com.whisk" %% "docker-testkit-impl-spotify" % "0.9.5" % Test
//libraryDependencies += "com.whisk" %% "docker-testkit-impl-docker-java" % "0.9.7" % Test

libraryDependencies += "com.spotify" % "docker-client" % "8.11.7"

libraryDependencies += "org.postgresql" % "postgresql" % "42.2.4"
libraryDependencies += "com.typesafe.slick" %% "slick" % "3.2.3"

libraryDependencies += "com.dimafeng" %% "testcontainers-scala" % "0.19.0" % Test
libraryDependencies += "org.testcontainers" % "postgresql" % "1.8.1"




