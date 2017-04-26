package cn.jw.rms.ab.micro.pred.wsfweight

import java.io.File

import com.typesafe.config.ConfigFactory
import org.apache.commons.lang.mutable.Mutable
import org.apache.spark.{SparkContext, SparkConf}
import org.joda.time.{DateTime, Days}

import scala.collection.mutable


object Test {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("RmsDataFramework")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").setMaster("local")
    val sc = new SparkContext(sparkConf)
    val conf_path = "./conf/application.conf"
    val conf = ConfigFactory.parseFile(new File(conf_path))
//    val exclude_liveDt = conf.getString("exclude_liveDt").split(",")
//    println(exclude_liveDt.contains("a"))
//    val  map = mutable.Map.empty[String,Int]
//    map += ("hello" -> 1)
//    println(map.get("hello"))

    val pre = new Predictor
    pre.predict(sc,conf,None)
    sc.stop()
  }
}
