name := "rms-ab-micro-pred-shortterm"

version := "1.3"

scalaVersion := "2.10.6"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "1.5.1" % "provided" ,
  "org.apache.spark" % "spark-mllib_2.10" % "1.5.1" % "provided",
  "joda-time" % "joda-time" % "2.9",
  "org.joda" % "joda-convert" % "1.8.1"/*,
  "org.scalanlp" % "breeze_2.10" % "0.12"*///,
  //"org.scalanlp" % "breeze-natives_2.10" % "0.12",
  //"org.scalanlp" % "breeze-viz_2.10" % "0.12"
)

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
    