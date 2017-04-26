package cn.jw.rms.ab.etl.fittotal

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
    val cleaner = new Cleaner
    val start = TimerMeter.start("fit-total")
    cleaner.clean(sc, conf, None)
    val ms = TimerMeter.end("fit-total", start)
    println (s"Elapsed Time = $ms ms")
  }

}
