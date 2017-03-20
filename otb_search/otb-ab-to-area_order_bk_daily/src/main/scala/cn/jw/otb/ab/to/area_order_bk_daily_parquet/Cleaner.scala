package cn.jw.otb.ab.to.area_order_bk_daily_parquet

import java.io.Serializable

import cn.jw.rms.data.framework.common.rules.CleanerAssembly
import cn.jw.rms.data.framework.common.utils.HDFSUtil
import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.joda.time.{DateTime, Days}
import org.joda.time.format.DateTimeFormat

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
      val htlOrderDir = config.getString("htl-order-dir")
      val dictHtlDir = config.getString("dict-hotel-dir")
      val dt = config.getString("dt")
      val testOpen = config.getBoolean("test-open")
      val htlOrderPath = if (testOpen) htlOrderDir else addDtPath(htlOrderDir, dt)
      val outputDir = config.getString("dist-dir")
      val sqlContext = new SQLContext(sc)
      val htlOrderOriginal = sqlContext.read.parquet(htlOrderPath).rdd
      val dictHtlOriginal = sc.textFile(dictHtlDir).map(_.split("\t"))

      val htlOrderKeyValue = htlOrderOriginal.mapPartitions {
        par =>
          par.map {
            lines =>
              val hotelId = lines(colIndexOrderBkDailySum("hotel_id")).toString
              val orderStatus = lines(colIndexOrderBkDailySum("order_status")).toString
              val orderDt = lines(colIndexOrderBkDailySum("order_dt")).toString

              val liveDt = lines(colIndexOrderBkDailySum("live_dt")).toString

              val cancelDt = lines(colIndexOrderBkDailySum("cancel_dt")).toString
              val rns = lines(colIndexOrderBkDailySum("rns")).toString.toFloat
              val rev = lines(colIndexOrderBkDailySum("rev")).toString.toFloat
              val priceLevel = lines(colIndexOrderBkDailySum("price_level")).toString
              (hotelId, (orderStatus, orderDt, liveDt, cancelDt, priceLevel, rns, rev))
          }
      }.filter {
        case (hotelId, (orderStatus, orderDt, liveDt, cancelDt, priceLevel, rns, rev)) =>
          liveDt >= "2014-01-01"
      }

      val dictHtlKeyValue = dictHtlOriginal.mapPartitions {
        par =>
          par.map {
            dt =>
              val hotelId = dt(colIndexDictHotel("masterhotelid"))
              val cityId = dt(colIndexDictHotel("city"))
              val zoneId = dt(colIndexDictHotel("zone"))
              val starId = dt(colIndexDictHotel("star"))
              val locationId = dt(colIndexDictHotel("location"))
              (hotelId, (cityId, zoneId, starId, locationId))
          }
      }

      val finalData = htlOrderKeyValue.join(dictHtlKeyValue).mapPartitions {
        par =>
          par.map {
            case (hotelId, ((orderStatus, orderDt, liveDt, cancelDt, priceLevel, rns, rev), (cityId, zoneId, starId, locationId))) =>
              ((cityId, zoneId, starId, locationId, priceLevel, orderDt, liveDt, cancelDt), (rns, rev))
          }
      }

      val initialResult = finalData.reduceByKey {
        (x, y) =>
          val total = (x._1 + y._1, x._2 + y._2)
          total
      }.filter {
        case ((cityId, zoneId, starId, locationId, priceLevel, orderDt, liveDt, cancelDt), total) =>
          val liveTime = DateTime.parse(liveDt, DateTimeFormat.forPattern("yyyy-MM-dd"))
          val orderTime = DateTime.parse(orderDt, DateTimeFormat.forPattern("yyyy-MM-dd"))
          val advBkdays = Days.daysBetween(orderTime, liveTime).getDays
          advBkdays >= 0
      }.map {
        case ((city_id, zone_id, star, location_id, price_level, order_dt, live_dt, cancel_dt), total) =>
          val rns = total._1
          val rev = total._2
          s"$city_id#$zone_id#$star#$location_id#$price_level#$order_dt#$live_dt#$cancel_dt#$rns#$rev"
        //          orderDetail(city_id.toInt, zone_id.toInt, star.toInt, location_id, price_level.toInt, order_dt, live_dt, cancel_dt, rns, rev)
      }

      val result = if (RDD_repartition_num > 0) {
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
        result.saveAsTextFile(outputPath, classOf[com.hadoop.compression.lzo.LzopCodec])
      }

    } match {
      case Success(res) =>
        gotSucceed = true
        None
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

  private def colIndexDictHotel(colname: String): Int = colname match {
    case "hotel" => 0
    case "district" => 1
    case "location" => 2
    case "hotelname" => 3
    case "star" => 4
    case "city" => 5
    case "bookable" => 6
    case "zone" => 7
    case "adress" => 8
    case "brand" => 9
    case "diy_breakfast" => 10
    case "west_breakfast" => 11
    case "chi_breakfast" => 12
    case "masterhotelid" => 13
    case "glon" => 14
    case "glat" => 15
    case "hoteldesc" => 16
    case "hotelname_en" => 17
    case "status" => 18
    case "updatetime" => 19
  }

  private def colIndexOrderBkDailySum(colname: String): Int = colname match {
    case "hotel_id" => 0
    case "order_status" => 1
    case "order_dt" => 2
    case "live_dt" => 3
    case "cancel_dt" => 4
    case "price_level" => 5
    case "roomId" => 6
    case "rns" => 7
    case "rev" => 8
  }
  case class orderDetail(
  city_id: Int,
  zone_id:Int,
  star: Int,
  location_id: String,
  price_level: Int,
  order_dt: String,
  live_dt: String,
  cancel_dt: String,
  rns: Float,
  rev: Float)


  override def succeed: (Boolean, String) = (gotSucceed, errMsg)
}