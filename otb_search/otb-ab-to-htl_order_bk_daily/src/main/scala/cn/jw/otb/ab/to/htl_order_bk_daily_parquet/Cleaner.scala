package cn.jw.otb.ab.to.htl_order_bk_daily_parquet

import java.io.Serializable

import cn.jw.rms.data.framework.common.rules.CleanerAssembly
import cn.jw.rms.data.framework.common.utils.HDFSUtil
import com.typesafe.config.Config
import org.apache.spark.SparkContext

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
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
      val inputPathRoot = config.getString("input_dir_root")
      val dt = config.getString("dt")
      val testOpen = config.getBoolean("test-open")
      val inputPath = if (testOpen) inputPathRoot else addDtPath(inputPathRoot, dt)
      val outputDir = config.getString("dist-dir")
//      val orderLastStatus = sc.textFile(inputPath).map(_.split(fieldSeparator))
      val sqlContext = new SQLContext(sc)
      val orderLastStatus = sqlContext.read.parquet(inputPath).rdd
      val dictHtlDir = config.getString("dict-hotel-dir")
      val dictHtlOriginal =  sc.textFile(dictHtlDir).map(_.split("\t"))

      val dictHtlKeyValue  = dictHtlOriginal.map {
        dt =>
          val hotelIdChild = dt(colIndexDictHotel("hotel"))
          val hotelIdMaster = dt(colIndexDictHotel("masterhotelid"))
          (hotelIdChild,hotelIdMaster)
      }.collectAsMap()


      val htlOrderBk = orderLastStatus.flatMap {
        lines =>
          val hotelIdChild = lines(colIndexOrderLastStatus("hotel_id")).toString
          val hotelIdMaster =  if(dictHtlKeyValue.contains(hotelIdChild)) dictHtlKeyValue(hotelIdChild) else hotelIdChild
          val orderStatus = lines(colIndexOrderLastStatus("last_order_status")).toString
          val orderDt = lines(colIndexOrderLastStatus("order_dt")).toString
          val arrDt = lines(colIndexOrderLastStatus("arr_dt")).toString
          val dptDt = lines(colIndexOrderLastStatus("dpt_dt")).toString
          val rns = lines(colIndexOrderLastStatus("rns")).toString.toFloat
          val rev = lines(colIndexOrderLastStatus("rev")).toString.toFloat
          val cancelDt = lines(colIndexOrderLastStatus("cancel_dt")).toString
          val priceLevel = lines(colIndexOrderLastStatus("price_level")).toString.toInt
          val roomId = lines (colIndexOrderLastStatus("roomId")).toString.toInt
          val arrTime = DateTime.parse(arrDt, DateTimeFormat.forPattern("yyyy-MM-dd"))
          val dptTime = DateTime.parse(dptDt, DateTimeFormat.forPattern("yyyy-MM-dd"))
          val timeInterval = Days.daysBetween(arrTime, dptTime).getDays
          (0 until timeInterval).map { day =>
            val liveDt = arrTime.plusDays(day).toString("yyyy-MM-dd")
            val everyDayRns = rns / timeInterval
            val everyDayRev = rev / timeInterval
            ((hotelIdMaster, orderStatus, orderDt, liveDt, cancelDt, priceLevel,roomId), (everyDayRns, everyDayRev))
          }
      }
//            htlOrderBk.foreach(println)
      val result = htlOrderBk.groupByKey().map {
        case (key, value) =>
          val hotel_id_m = key._1
          val order_status = key._2
          val order_dt = key._3
          val live_dt = key._4
          val cancel_dt = key._5
          val price_level = key._6
          val sub_room_id = key._7
          val total = value.reduce {
            (x, y) => (x._1 + y._1, x._2 + y._2)
          }
          val rns = total._1
          val rev = total._2
        orderDetail(hotel_id_m.toInt,order_status,order_dt,live_dt,cancel_dt,price_level,sub_room_id,rns,rev)
      }
      val outputPath = if (testOpen) outputDir else addDtPath(outputDir, dt)
      if (!hdfsHost.equals("local")) {
        HDFSUtil.delete(hdfsHost, outputPath)
      } else {
        val out_dir = Path(outputPath)
        if (out_dir.exists) out_dir.deleteRecursively()
      }

      if (saveToFile) {
       val resultSchema = sqlContext.createDataFrame(result)
        resultSchema.write.parquet(outputPath)
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

  override def succeed: (Boolean, String) = (gotSucceed, errMsg)


  private def addDtPath(path: String, dt: String): String = {
    if (path.endsWith("/")) {
      path + dt
    } else {
      path + "/" + dt
    }
  }
  case class orderDetail(hotel_id_m:Int,order_status:String,order_dt:String,live_dt:String,cancel_dt:String,price_level:Int,sub_room_id:Int,rns:Float,rev:Float)
  private def colIndexOrderLastStatus(colname: String): Int = colname match {
    case "order_id" => 0
    case "hotel_id" => 1
    case "last_order_status" => 2
    case "order_dt" => 3
    case "arr_dt" => 4
    case "dpt_dt" => 5
    case "cancel_dt" => 6
    case "price_level" => 7
    case "roomId" => 8
    case "rns" => 9
    case "rev" => 10
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
    case "adress"=> 8
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
}