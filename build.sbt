name := "skiis"

version := "2.0.2-SNAPSHOT"

organization := "org.alexboisvert"

scalaVersion := "2.12.8"

crossScalaVersions := Seq("2.12.8", "2.13.0")

libraryDependencies ++= Seq(
  "junit" % "junit" % "4.11" % Test,
  "org.scalatest" %% "scalatest" % "3.0.8" % Test,
  "org.scalacheck" %% "scalacheck" % "1.14.0" % Test
)
