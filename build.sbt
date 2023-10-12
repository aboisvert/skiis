name := "skiis"

version := "0.1.0"

organization := "org.alexboisvert"

scalaVersion := "2.13.12"

//crossScalaVersions := Seq("2.10.4", "2.11.1")

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.2.17" % "test",
  "org.scalacheck" %% "scalacheck" % "1.15.4" % "test"
)
