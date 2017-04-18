name := "rms-data-framework"

version := "1.0"

scalaVersion := "2.10.6"

lazy val frameworkCommon = project.in(file("modules/framework-common"))

lazy val frameworkCore = project.in(file("modules/framework-core")).aggregate(frameworkCommon)
  .dependsOn(frameworkCommon)

lazy val root = project.in(file("."))

//assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)