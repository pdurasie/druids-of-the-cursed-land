ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.1.1"

lazy val root = (project in file("."))
  .settings(
    name := "scala"
  )

libraryDependencies += "org.locationtech.jts" % "jts-core" % "1.18.1"