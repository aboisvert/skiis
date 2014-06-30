name := "skiis"

version := "0.1.0"

organization := "org.alexboisvert"

scalaVersion := "2.11.1"

crossScalaVersions := Seq("2.10.4", "2.11.1")

libraryDependencies ++= Seq(
  "junit" % "junit" % "4.11" % "test",
  "org.scalatest" %% "scalatest" % "2.2.0" % "test",
  "org.scalacheck" %% "scalacheck" % "1.11.4" % "test"
)
