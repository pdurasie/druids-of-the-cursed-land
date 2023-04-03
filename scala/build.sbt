ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.0"

scalacOptions += "-Ypartial-unification"

lazy val root = (project in file("."))
  .settings(name := "scala")
  .aggregate(common, osm_processing, spawning) // Add this line to include all the subprojects in the root project

lazy val common = (project in file("common"))
  .settings(
    name := "common",
    libraryDependencies += "org.locationtech.jts" % "jts-core" % "1.19.0",
    libraryDependencies += "org.tpolecat" %% "doobie-core" % "1.0.0-RC2",
    libraryDependencies += "org.tpolecat" %% "doobie-postgres" % "1.0.0-RC2",
    libraryDependencies += "org.locationtech.spatial4j" % "spatial4j" % "0.8",
    libraryDependencies += "org.typelevel" %% "cats-core" % "2.9.0"
  )

// OSM processing module
lazy val osm_processing = (project in file("osm_processing"))
  .dependsOn(common)
  .settings(
    name := "osm_processing"
    // Add any additional settings and library dependencies specific to this module
  )

// Spawning module
lazy val spawning = (project in file("spawning"))
  .dependsOn(common)
  .settings(
    name := "spawning"
    // Add any additional settings and library dependencies specific to this module
  )

enablePlugins(PackPlugin)
packMain := Map("process-osm" -> "OsmDataProcessor", "spawn-herbs" -> "Spawner")
