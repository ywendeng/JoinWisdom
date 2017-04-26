name := "rms-ab-micro-pred-gen-config"

version := "3.0"

scalaVersion := "2.10.6"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "1.5.1" % "provided" ,
  "org.apache.spark" % "spark-sql_2.10" % "1.5.1" % "provided",
  "mysql" % "mysql-connector-java" % "5.1.38",
  "joda-time" % "joda-time" % "2.9",
  "org.joda" % "joda-convert" % "1.8.1"
)

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
    