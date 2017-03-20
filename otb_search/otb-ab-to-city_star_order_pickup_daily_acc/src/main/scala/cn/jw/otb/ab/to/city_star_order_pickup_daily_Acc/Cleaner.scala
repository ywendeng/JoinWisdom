package cn.jw.otb.ab.to.city_star_order_pickup_daily_Acc

import java.io.Serializable

import cn.jw.rms.data.framework.common.rules.CleanerAssembly
import cn.jw.rms.data.framework.common.utils.HDFSUtil
import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD


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
      val RDD_repartition_num = config.getInt("RDD-repartition-num")
      val pkDailyOrderDir = config.getString("pickupDaily-order-dir")
      val dt = config.getString("dt")
      val testOpen = config.getBoolean("test-open")
      val pickupDailyPath = if (testOpen) pkDailyOrderDir else addDtPath(pkDailyOrderDir, dt)
      val outputDir = config.getString("dist-dir")

      val pkDailyOrderOriginal = sc.textFile(pickupDailyPath).map(_.split(fieldSeparator))

      val pkDailyOrderData = pkDailyOrderOriginal.map {
        line =>
          val cityId = line(colIndexAdvBkDay("cityId"))
          val starId = line(colIndexAdvBkDay("starId"))
          val liveDt = line(colIndexAdvBkDay("liveDt"))
          val rns = line(colIndexAdvBkDay("rns")).toFloat
          val rev = line(colIndexAdvBkDay("rev")).toFloat
          val advBkDays = line(colIndexAdvBkDay("advBkDays")).toInt
          ((cityId, starId, liveDt), (advBkDays, rns, rev))
      }

      val initialResult = pkDailyOrderData.groupByKey.flatMap {
        case (key, value) =>
          val cityId = key._1
          val starId = key._2
          val liveDt = key._3
          val sortAdvBkDays = value.toArray.sortBy { case (advBkDays, rns, rev) => advBkDays }.reverse
          val scanSum = sortAdvBkDays.scanLeft((0, 0f, 0f)) {
            (a, b) =>
              val total = (b._1, a._2 + b._2, a._3 + b._3)
              total
          }.tail.map {
            lines =>
              val advBkDay = lines._1
              val rns = lines._2
              val rev = lines._3
              val remark = ""
              s"$cityId#$starId#$remark#$liveDt#$advBkDay#$rns#$rev"
          }
          scanSum
      }

      val result = if (!RDD_repartition_num.equals(1)) {
        initialResult.repartition(RDD_repartition_num)
      } else initialResult

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
    case "starId" => 1
    case "remark" => 2
    case "liveDt" => 3
    case "advBkDays" => 4
    case "rns" => 5
    case "rev" => 6
  }

  override def succeed: (Boolean, String) = (gotSucceed, errMsg)
}