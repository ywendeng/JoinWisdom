name := "otb-ab-to-city_zone_order_pickup_daily"

version := "1.0"

scalaVersion := "2.10.6"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "1.5.1" % "provided" ,
  "joda-time" % "joda-time" % "2.9",
  "org.joda" % "joda-convert" % "1.8.1"
)

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
