package cn.jw.rms.ab.pred.fix

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

    /*val sc = new SparkContext(sparkConf)
    val predSegConfDistinct = sc.parallelize(Array((("h1","s1"), 1),(("h1","s2"), 1),(("h1","s3"), 1),(("h2","s10"), 1),(("h2","s11"), 1),(("h3","s20"), 1)))
    val htlSumRns = sc.parallelize(Array(("h1", "v1"),("h1", "v2"),("h1", "v3"),("h2", "v10"),("h2", "v11"),("h3", "v20")))

    val htlSumRnsWithSeg = predSegConfDistinct.keys.join(htlSumRns).mapPartitions {
      par =>
        par.map {
          case (htlcd, (segcd, values)) =>
            ((htlcd, segcd), values)
        }
    }

    println("htlSumRnsWithSeg")
    htlSumRnsWithSeg.collect().foreach{
      case ((htlcd, segcd), values) =>
        println(s"$htlcd#$segcd#$values")
    }

    val subData = sc.parallelize(Array((("h1","s1") ,"v1"),(("h1","s1") ,"v2"),(("h1","s2") ,"v1")))
    val result = htlSumRnsWithSeg.subtract(subData)

    println("result")
    result.collect().foreach{
      case ((htlcd, segcd), values) =>
        println(s"$htlcd#$segcd#$values")
    }*/

  }
}
