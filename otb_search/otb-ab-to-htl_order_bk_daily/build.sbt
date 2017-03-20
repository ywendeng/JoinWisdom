name := "otb-ab-to-htl_order_bk_daily"

version := "1.1"

scalaVersion := "2.10.6"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "1.5.1" /*% "provided"*/,
  "joda-time" % "joda-time" % "2.9",
  "org.joda" % "joda-convert" % "1.8.1",
  "org.apache.spark" % "spark-sql_2.10" % "1.5.1" /*% "provided"*/
)

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
