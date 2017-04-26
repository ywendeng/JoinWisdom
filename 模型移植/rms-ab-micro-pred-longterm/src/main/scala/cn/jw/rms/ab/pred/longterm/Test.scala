package cn.jw.rms.ab.pred.longterm

import java.io.File

import cn.jw.rms.data.framework.common.utils.TimerMeter
import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext}


object Test {

  def main(args: Array[String]): Unit = {

    //List(List(1, 2), List(5, 5), List(9, 4), List(6, 3)).transpose.map(_.sum).foreach(println)
    val sparkConf = new SparkConf().setAppName("RmsDataFramework")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").setMaster("local")

    val file = new File("conf/application.conf")
    println("config file path = " + file.getAbsolutePath)
    val conf = ConfigFactory.parseFile(file)

    val sc = new SparkContext(sparkConf)

   /* val rdd1 = sc.parallelize(List(("a", (11, 1)), ("a",(12, 3)), ("b",(10, 1))))
    rdd1
      .aggregateByKey((0.0, 0))(
        {
          case ((sum1, count1), (v1, v2)) =>
            (sum1 + v1, count1+1)
        },
        {
          case ((sum1, count),
          (otherSum1, otherCount)) =>
            (sum1 + otherSum1, count + otherCount)
        }
      )
      .map {
        case (k, (sum1, count1)) => (k, (sum1/count1, count1))
      }
      .collect().foreach(println)*/

    val pred = new Predictor

    val start = TimerMeter.start("longterm")
    pred.predict(sc, conf, None)
    val ms = TimerMeter.end("longterm", start)
    println (s"Estimated Time = $ms ms")

    /*val dt = new DateTime("2016-04-25")
    println(dt.getDayOfWeek)*/

  }

}
