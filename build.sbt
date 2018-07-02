name := "spark"

version := "0.1"

// need to specify all 3 parts of scalaVersion
scalaVersion := "2.11.12"

// automatically appends to scalaVersion to 2nd part
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.1"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.3.1"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % Test
