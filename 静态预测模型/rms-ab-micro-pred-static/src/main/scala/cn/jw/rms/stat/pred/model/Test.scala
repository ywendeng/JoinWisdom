package cn.jw.rms.stat.pred.model

import java.io.File

import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by deanzhang on 16/2/25.
  */
object Test {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("RmsDataFramework")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").setMaster("local")

    val file = new File("conf/application.conf")
    println("config file path = " + file.getAbsolutePath)
    val conf = ConfigFactory.parseFile(file)

    val sc = new SparkContext(sparkConf)
    val pred = new Predictor

    pred.predict(sc, conf, None)

  }

}
