package cn.jw.rms.ab.pred.combine

import java.io.File

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

    pred.predict(sc, conf, None)

    /*val a = Array.ofDim[Double](10)
    println(a.mkString("#"))*/

    /*println(math.round(1.44444444444444))
    println(math.round(0.5076909182))*/
  }
//1:0,1;3:0.05,0.95;7:0.15,0.85;14:0.2,0.8;21:0.4,0.6;28:0.5,0.5;42:0.8,0.2;365:0.95,0.05
}
