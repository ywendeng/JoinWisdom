package cn.jw.rms.pred.genconfig

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
    val pred = new Cleaner
    val start = TimerMeter.start("gen-config")
    pred.clean(sc, conf, None)
    val ms = TimerMeter.end("gen-config", start)
    println (s"Elapsed Time = $ms ms")
  }

}

//spark-submit --class cn.jw.rms.ab.pred.shortterm.hourly.Test --master local[2] --driver-memory 4G --conf spark.shuffle.consolidateFiles=true --conf spark.rdd.compress=true ./rms-ab-micro-pred-shortterm-hourly-assembly-1.3.jar