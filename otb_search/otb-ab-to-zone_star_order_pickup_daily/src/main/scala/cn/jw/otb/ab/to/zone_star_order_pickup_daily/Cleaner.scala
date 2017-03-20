package cn.jw.otb.ab.to.zone_star_order_pickup_daily

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
      val RDD_repartition_num = config.getInt("RDD-repartition-num")
      val areaOrderDir = config.getString("area-order-dir")
      val cancelOrderDir = config.getString("cancel-order-dir")
      val dt = config.getString("dt")
      val testOpen = config.getBoolean("test-open")
      val areaOrderPath = if (testOpen) areaOrderDir else addDtPath(areaOrderDir, dt)
      val cancelOrderPath = if (testOpen) cancelOrderDir else addDtPath(cancelOrderDir, dt)
      val outputDir = config.getString("dist-dir")

      val areaOrderOriginal = sc.textFile(areaOrderPath).map(_.split(fieldSeparator))
      val cancelOrder = sc.textFile(cancelOrderPath).map(_.split(fieldSeparator))

      val advBkDayDataKv = areaOrderOriginal.map {
        line =>
          val zoneId = line(colIndexCityZoneOrderPk("zone_id"))
          val starId = line(colIndexCityZoneOrderPk("star_id"))
          val liveDt = line(colIndexCityZoneOrderPk("live_dt"))
          val liveDtFormat =  DateTime.parse(liveDt, DateTimeFormat.forPattern("yyyy-MM-dd"))
          val orderDt = line(colIndexCityZoneOrderPk("order_dt"))
          val orderDtFormat = DateTime.parse(orderDt, DateTimeFormat.forPattern("yyyy-MM-dd"))
          val advBkDay = Days.daysBetween(orderDtFormat, liveDtFormat).getDays
          val rns = line(colIndexCityZoneOrderPk("rns")).toFloat
          val rev = line(colIndexCityZoneOrderPk("rev")).toFloat
          ((zoneId, starId, liveDt,advBkDay), (rns, rev))
      }.reduceByKey {
        (x, y) => (x._1 + y._1, x._2 + y._2)
      }
      val advBkDayData = advBkDayDataKv.map {
        case  ((zoneId, starId, liveDt,advBkDay), (rns, rev)) =>
          ((zoneId, starId, liveDt), (advBkDay,rns, rev))
      }.groupByKey.flatMap{
        case (( zoneId,starId,liveDt),value)=>
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
                if (advBkSet.contains(adv)) ((zoneId, starId, liveDt, adv), rnsRevSet(adv))
                else ((zoneId, starId, liveDt, adv), (0f, 0f))
              }else{
                if (advBkSet.contains(adv)) ((zoneId, starId, liveDt, 0), rnsRevSet(adv))
                else ((zoneId, starId, liveDt, 0), (0f, 0f))
              }
          }
      }

      val adcBkDayTotal = advBkDayData.reduceByKey {
        (x, y) =>(x._1 + y._1, x._2 + y._2)
      }.map {
        case ((zoneId,starId, liveDt, advBkDay), total) =>
          val rns = total._1
          val rev = total._2
          ((zoneId,starId, liveDt, advBkDay), (rns, rev))
      }


      val cancelOrderKv = cancelOrder.map {
        dt =>
          val starId = dt(colIndexAdvCancelDay("starId"))
          val zoneId = dt(colIndexAdvCancelDay("zoneId"))
          val liveDt = dt(colIndexAdvCancelDay("liveDt"))
          val advCcDay = dt(colIndexAdvCancelDay("advCcDay")).toInt
          val rns = dt(colIndexAdvCancelDay("rns")).toFloat
          val rev = dt(colIndexAdvCancelDay("rev")).toFloat
          ((zoneId,starId, liveDt, advCcDay), (rns, rev))
      }

      val initialResult = adcBkDayTotal.leftOuterJoin(cancelOrderKv).map {
        case ((zoneId,starId, liveDt, advDay), (x, y)) =>
          val value = y.getOrElse((0f, 0f))
          val rns = x._1 - value._1
          val rev = x._2 - value._2
          val remark = ""
          s"$zoneId#$starId#$remark#$liveDt#$advDay#$rns#$rev"
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

  private def colIndexAdvCancelDay(colname: String): Int = colname match {
    case "zoneId" => 0
    case "starId" => 1
    case "remark" => 2
    case "liveDt" => 3
    case "advCcDay" => 4
    case "rns" => 5
    case "rev" => 6
  }

  override def succeed: (Boolean, String) = (gotSucceed, errMsg)
}