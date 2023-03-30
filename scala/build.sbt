ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.1.1"

lazy val root = (project in file("."))
  .settings(name := "scala")

libraryDependencies += "org.locationtech.jts" % "jts-core" % "1.19.0"
libraryDependencies += "org.tpolecat" %% "doobie-core" % "1.0.0-RC2"
libraryDependencies += "org.tpolecat" %% "doobie-postgres" % "1.0.0-RC2"
