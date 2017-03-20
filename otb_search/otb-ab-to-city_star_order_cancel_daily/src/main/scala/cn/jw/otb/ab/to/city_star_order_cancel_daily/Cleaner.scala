package cn.jw.otb.ab.to.city_star_order_cancel_daily

import java.io.Serializable

import cn.jw.rms.data.framework.common.utils.HDFSUtil
import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.joda.time.{DateTime, Days}
import org.joda.time.format.DateTimeFormat
import cn.jw.rms.data.framework.common.rules.CleanerAssembly

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
      val areaOrderDir = config.getString("area-order-dir")
      val dt = config.getString("dt")
      val testOpen = config.getBoolean("test-open")
      val areaOrderPath = if (testOpen) areaOrderDir else addDtPath(areaOrderDir, dt)
      val outputDir = config.getString("dist-dir")

      val areaOrderOriginal = sc.textFile(areaOrderPath).map(_.split(fieldSeparator))
      val areaOrderFilterCancel = areaOrderOriginal.filter {
        lines =>
          val cancelDt = lines(colIndexCityZoneOrderPk("cancel_dt"))
          cancelDt != ""
      }
      val advCcDayDataKv = areaOrderFilterCancel.map {
        line =>
          val cityId = line(colIndexCityZoneOrderPk("city_id"))
          val starId = line(colIndexCityZoneOrderPk("star_id"))
          val liveDt = line(colIndexCityZoneOrderPk("live_dt"))
          val liveDtFormat =  DateTime.parse(liveDt, DateTimeFormat.forPattern("yyyy-MM-dd"))
          val cancelDt = line(colIndexCityZoneOrderPk("cancel_dt"))
          val cancelDtFormat = DateTime.parse(cancelDt, DateTimeFormat.forPattern("yyyy-MM-dd"))
          val advCcDay = Days.daysBetween(cancelDtFormat, liveDtFormat).getDays
          val rns = line(colIndexCityZoneOrderPk("rns")).toFloat
          val rev = line(colIndexCityZoneOrderPk("rev")).toFloat
          ((cityId, starId, liveDt,advCcDay), (rns, rev))
      }.reduceByKey {
        (x, y) => (x._1 + y._1, x._2 + y._2)
      }
      val advCcDayData = advCcDayDataKv.map {
        case  ((cityId, starId, liveDt,advCcDay), (rns, rev)) =>
          ((cityId, starId, liveDt), (advCcDay,rns, rev))
      }.groupByKey.flatMap{
        case (( cityId,starId,liveDt),value)=>
          val advBkSet = value.map(_._1.toInt).toArray
          val rnsRevSet  = value.map{
            lines=>
              (lines._1.toInt,(lines._2,lines._3))
          }.toMap
          val advMax = advBkSet.max
          val advMin = advBkSet.min
          (advMin to advMax).map{
            adv=>
              if (adv >= 0) {
                if (advBkSet.contains(adv)) ((cityId, starId, liveDt, adv), rnsRevSet(adv))
                else ((cityId, starId, liveDt, adv), (0f, 0f))
              }else{
                if (advBkSet.contains(adv)) ((cityId, starId, liveDt, 0), rnsRevSet(adv))
                else ((cityId, starId, liveDt, 0), (0f, 0f))
              }
          }
      }

      val initialResult = advCcDayData.reduceByKey {
        (x, y) => (x._1 + y._1, x._2 + y._2)
      }.map {
        case ((cityId, starId, liveDt, advCcDay), total) =>
          val rns = total._1
          val rev = total._2
          val remark = ""
          s"$cityId#$starId#$remark#$liveDt#$advCcDay#$rns#$rev"
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


  private def colIndexCityZoneOrderPk(colname: String): Int = colname match {
    case "city_id" => 0
    case "zone_id" => 1
    case "star_id" => 2
    case "location_id" => 3
    case "price_level" => 4
    case "order_dt" => 5
    case "live_dt" => 6
    case "cancel_dt" => 7
    case "rns" => 8
    case "rev" => 9
  }

  override def succeed: (Boolean, String) = (gotSucceed, errMsg)
}