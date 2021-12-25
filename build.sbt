val scala3Version = "3.1.0"

lazy val root = project
  .in(file("."))
  .settings(
    name := "scala-lab",
    version := "0.1.0-SNAPSHOT",

    scalaVersion := scala3Version,

    libraryDependencies ++= Seq(
      "dev.zio" %% "zio" % "2.0.0-RC1"
    )
  )
