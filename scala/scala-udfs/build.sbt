ThisBuild / scalaVersion     := "2.13.11"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.example"
ThisBuild / organizationName := "example"

lazy val root = (project in file("."))
  .settings(
    name := "scala-udfs",
    libraryDependencies ++= List(
      "org.apache.spark" %% "spark-sql" % "3.4.1" % "provided",
      "org.scalatest" %% "scalatest" % "3.2.15" % Test
    )
  )
