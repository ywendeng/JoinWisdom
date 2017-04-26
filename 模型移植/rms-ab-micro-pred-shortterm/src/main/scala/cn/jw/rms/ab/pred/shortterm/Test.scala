package cn.jw.rms.ab.pred.shortterm

import java.io.File

import cn.jw.rms.data.framework.common.utils.TimerMeter
import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext}


object Test {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("RmsDataFramework")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").setMaster("local")

    val file = new File("conf/application.conf")
    println("config file path = " + file.getAbsolutePath)
    val conf = ConfigFactory.parseFile(file)

    val sc = new SparkContext(sparkConf)
    val pred = new Predictor
    val start = TimerMeter.start("shortterm")
    pred.predict(sc, conf, None)
    val ms = TimerMeter.end("shortterm", start)
    println (s"Elasped Time = $ms ms")
  }

}
