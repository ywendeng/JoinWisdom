name := "rms-ab-micro-pred-static"

version := "1.0"

scalaVersion := "2.10.6"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "1.5.1" % "provided" ,
  "joda-time" % "joda-time" % "2.9",
  "org.joda" % "joda-convert" % "1.8.1",
  "mysql" % "mysql-connector-java" % "5.1.38",
  "com.zaxxer" % "HikariCP" % "2.4.6",
  "org.scalaj" %% "scalaj-http" % "2.3.0"
)
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)