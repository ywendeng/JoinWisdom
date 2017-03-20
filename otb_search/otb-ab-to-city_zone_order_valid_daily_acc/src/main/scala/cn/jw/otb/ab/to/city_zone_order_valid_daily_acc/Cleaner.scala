package cn.jw.otb.ab.to.city_zone_order_valid_daily_acc

import java.io.Serializable

import cn.jw.rms.data.framework.common.rules.CleanerAssembly
import cn.jw.rms.data.framework.common.utils.HDFSUtil
import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, Days}

import scala.reflect.io.Path
import scala.util.{Failure, Success, Try}


class Cleaner extends CleanerAssembly with Serializable {


  var gotSucceed = false
  var errMsg = ""

  override def clean(sc: SparkContext, config: Config, prevStepRDD: Option[RDD[String]]): Option[RDD[String]] = {
    Try {
      val hdfsHost = config.getString("hdfsHost")
      val saveToFile = config.getBoolean("save-result-to-file")
      val fieldSeparator = config.getString("field-splitter")
      val pkDailyOrderValidDir = config.getString("validDaily-order-dir")
      val dt = config.getString("dt")
      val testOpen = config.getBoolean("test-open")
      val areaOrderPath = if (testOpen) pkDailyOrderValidDir else addDtPath(pkDailyOrderValidDir, dt)
      val outputDir = config.getString("dist-dir")

      val pkDailyOrderValidOriginal = sc.textFile(areaOrderPath).map(_.split(fieldSeparator))

      val pkDailyOrderValidData = pkDailyOrderValidOriginal.map {
        line =>
          val cityId = line(colIndexAdvBkDay("cityId"))
          val zoneId = line(colIndexAdvBkDay("zoneId"))
          val liveDt = line(colIndexAdvBkDay("liveDt"))
          val rns = line(colIndexAdvBkDay("rns")).toFloat
          val rev = line(colIndexAdvBkDay("rev")).toFloat
          val advBkDays = line(colIndexAdvBkDay("advBkDays")).toInt
          ((cityId, zoneId, liveDt), (advBkDays,rns, rev))
      }

     val result = pkDailyOrderValidData.groupByKey.flatMap{
       case(key,value)=>
         val cityId = key._1
         val zoneId = key._2
         val liveDt = key._3
         val sortAdvBkDays = value.toArray.sortBy {case(advBkDays,rns, rev)=> advBkDays }.reverse
         val scanSum = sortAdvBkDays.scanLeft((0,0f,0f)){
           (a,b)=>
           val total = (b._1,a._2 + b._2 ,a._3 + b._3)
             total
         }.tail.map{
           lines=>
             val advBkDay = lines._1
             val rns = lines._2
             val rev = lines._3
             val remark = ""
             s"$cityId#$zoneId#$remark#$liveDt#$advBkDay#$rns#$rev"
         }
        scanSum
     }

      val outputPath = if (testOpen) outputDir else addDtPath(outputDir, dt)
      if (!hdfsHost.equals("local")) {
        HDFSUtil.delete(hdfsHost, outputPath)
      } else {
        val out_dir = Path(outputPath)
        if (out_dir.exists) out_dir.deleteRecursively()
      }

      if (saveToFile) {
        result.saveAsTextFile(outputPath)
      }

      result

    } match {
      case Success(res) =>
        gotSucceed = true
        Some(res)
      case Failure(e) =>
        gotSucceed = false
        errMsg = e.getMessage
        e.printStackTrace()
        None

    }

  }

  private def addDtPath(path: String, dt: String): String = {
    if (path.endsWith("/")) {
      path + dt
    } else {
      path + "/" + dt
    }
  }



  private def colIndexAdvBkDay(colname: String): Int = colname match {
    case "cityId" => 0
    case "zoneId" => 1
    case "remark" => 2
    case "liveDt" => 3
    case "advBkDays" => 4
    case "rns" => 5
    case "rev" => 6
  }

  override def succeed: (Boolean, String) = (gotSucceed, errMsg)
}