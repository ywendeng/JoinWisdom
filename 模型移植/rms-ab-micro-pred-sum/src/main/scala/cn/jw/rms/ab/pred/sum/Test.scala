package cn.jw.rms.ab.pred.sum

import java.io.File

import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext}


object Test {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("RmsDataFramework")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").setMaster("local")
    val sc = new SparkContext(sparkConf)
    val conf = ConfigFactory.parseFile(new File("conf/application.conf"))
/*
    val s1 = sc.parallelize(Array((1,1),(1,1),(1,1),(2,1),(2,1),(3,1),(3,1),(3,1),(4,1),(4,1),(5,1),(5,1),(6,1),(7,1),(8,1)))
    val s2 = sc.parallelize(Array((1,2),(2,3),(3,4),(4,5),(5,6),(6,7),(7,8),(8,9)))
    val res = s1.leftOuterJoin(s2).map{
      case (key, (s1v, s2v)) =>
        s"$key#$s1v#$s2v"
    }
    res.collect().foreach(println)*/
    val pre = new Predictor
    pre.predict(sc,conf,None)
    sc.stop()
  }
}
