package cn.jw.otb.ab.to.city_star_order_cancel_daily

import java.io.File

import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext}


object Test {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("RmsDataFramework")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").setMaster("local")

    val file = new File("conf/application.conf")
    println("config file path = " + file.getAbsolutePath)
    val conf = ConfigFactory.parseFile(file)

    val sc = new SparkContext(sparkConf)
    val pred = new Cleaner
    pred.clean(sc, conf, None)
    sc.stop()
  }
}
