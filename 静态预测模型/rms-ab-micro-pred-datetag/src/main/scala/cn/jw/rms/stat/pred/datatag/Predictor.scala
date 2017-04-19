package cn.jw.rms.stat.pred.datatag



import collection.JavaConversions._
import cn.jw.rms.data.framework.common.rules.PredictModelAssembly
import cn.jw.rms.data.framework.common.utils.HDFSUtil
import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, Days}

import scala.util.{Failure, Success, Try}
import scala.reflect.io.Path

/**
  * Created with IntelliJ IDEA.
  * Author:DengYuanWen
  * Date:2016/7/26
  * Time:15:44
  * Email:dengyw@jointwisdom.cn
  */
/**
  * 该类主要用于生成DataTag表，可以根据该表映射特定时间对应的tag(标签)记录
  */
class Predictor  extends PredictModelAssembly with Serializable {

  var gotSucceed = false
  var errMsg = ""

  override def accuracy(sc: SparkContext, config: Config, prevStepRDD: Option[RDD[String]]): String = "Not Supported"

  override def succeed: (Boolean, String) = (gotSucceed, errMsg)

  override def predict(sc: SparkContext, config: Config, prevStepRDD: Option[RDD[String]]): Option[RDD[String]] = {
    Try {
    val seasonDir = config.getString("season-dir")
    val holidayDir = config.getString("holidays-dir")
    val regex = config.getString("field-splitter")
    val distDir = config.getString("dist-dir")
    val hotelDir = config.getString("hotel-dir")
    val hadoopHost = config.getString("hadoop-host")
    val histEnable = config.getBoolean("hist-enable")
    val histStart = config.getString("hist-start")
    val histEnd = config.getString("hist-end")
    val fcDays = config.getInt("fc-days")
    val fcDt = config.getString("fc-dt")
    val histDays = config.getInt("hist-days")
    val seasonWeekTagType = config.getString("season-weekday-tag-type")
    val weekendDays = config.getIntList("weekend-days").map(_.toInt).toArray
    /**
      * 切分得到相应的季度和节假日数据集对应的数组
      */
    val commonHtlcd = config.getString("common-htlcd")
    val fcHtl = config.getString("fc-htlcd")
    val seasonRaw = sc.textFile(seasonDir).map(_.split(regex, -1)).collect()
    val holidayRaw = sc.textFile(holidayDir).map(_.split(regex, -1)).collect()
    val hotelRaw=sc.textFile(hotelDir).map(_.split(regex, -1)).collect()
    /**
      * 从配置文件中读取需要产生tag的开时间和结束时间
      * 之间的天数
      */


      val (fcStartDt,endDt)= if (histEnable) {
      val startDtFormat = DateTime.parse(histStart, DateTimeFormat.forPattern("yyyy-MM-dd"))
      val endDtFormat = DateTime.parse(histEnd, DateTimeFormat.forPattern("yyyy-MM-dd"))
      //intervalDays =
        (startDtFormat,endDtFormat)
    } else {
        val fc_dt=if(fcDt =="now")
         new DateTime(DateTime.now().toString("yyyy-MM-dd")).minusDays(1)
        else
          DateTime.parse(fcDt, DateTimeFormat.forPattern("yyyy-MM-dd"))

      val histTime: DateTime = fc_dt.minusDays(histDays)
      val featureTime = fc_dt.plusDays(fcDays)
        (histTime,featureTime)
    }
      val intervalDays = Days.daysBetween(fcStartDt,endDt).getDays


    val allHotelCd =hotelRaw.filter{
      r =>
        if (fcHtl == "all")
          r(hotelColIdx("htl_cd")) != commonHtlcd
        else {
          val htlList = fcHtl.split(",")
          htlList.contains(r(hotelColIdx("htl_cd")))
        }
    }.map{
      line =>
        val htl_cd = line(hotelColIdx("htl_cd"))
        htl_cd
    }.distinct

    /**
      * 将时间间隔序列转换为数组
      */
    val intervalDaysCount = (0 to intervalDays).toArray

    /**
      * 对所有酒店的在指定时间范围内的每一天时间进行加上tag
      */
  val allHtlResult = allHotelCd.flatMap{
      htl_cd =>
      /**
        *选择出该酒店在season记录表中对应的数据集
        */
       val htlSeason= seasonRaw.filter{
         line=>
           line(seasonColIdx("htl_cd"))== htl_cd
       }

        val result = intervalDaysCount.map(dayNum => {
          /**
            * 根据天数计算出日期
            */
          val date =fcStartDt.plusDays(dayNum)
          val tag = tagGenerate(date, htlSeason, holidayRaw,seasonWeekTagType,weekendDays,htl_cd)

          val seasonTag = tag._1
          val specialTag = tag._2

          /**
            * 将日期转换为yyyy-MM-dd的形式并和生成的tag封装为字符串的形式
            */
          val dateFormat=date.toString.substring(0,10)
          if (specialTag.isEmpty) {
            s"$htl_cd#$dateFormat#$seasonTag#"
          } else {
            val strSpecialTag = specialTag.get.mkString(",")
            s"$htl_cd#$dateFormat#$seasonTag#$strSpecialTag"
          }
        })
        result
    }

    /**
      * 将Array[RDD[String]]格式数据集 转换为RDD String
      */
    if (allHtlResult.nonEmpty) {
      if (distDir.startsWith("hdfs://")) {
        HDFSUtil.delete(hadoopHost, distDir)
      } else {
        val path: Path = Path(distDir)
        path.deleteRecursively()
      }
      sc.parallelize(allHtlResult).saveAsTextFile(distDir)
    }

    } match {
      case Success(res) =>
        gotSucceed = true
        None
      case Failure(e) =>
        gotSucceed = false
        errMsg = e.toString
        println(errMsg)
        e.printStackTrace()
        None
    }
  }


  private def seasonWeekday2Tag(seasonNum: Int, weekday: Int) = {
    (seasonNum + 1) * 10 + weekday
  }
  private def seasonWeekday2Tag(seasonNum: Int) = {
    (seasonNum + 1) * 10
  }

  private def seasonWeekday2Tag(seasonNum: Int, weekday: Int, weekendDays: Array[Int]) = {
    val dayTag = if (weekendDays.contains(weekday)) 1 else 2
    (seasonNum + 1) * 10 + dayTag
  }

  /**
    *  生成一个lvdt 对应的tag
    *
    * @param lvdt  入住日期
    * @param seasonRows  季度数据集
    * @param specialDaysRows 特殊节假日数据数据集
    * @param seasonWeekTagType 产生tag 计算方式
    * @param weekendDays 周末的时间天数
    * @param htlcd 酒店ID
    * @return
    */
  def  tagGenerate(lvdt: DateTime,
             seasonRows: Array[Array[String]],
             specialDaysRows: Array[Array[String]],
             seasonWeekTagType: String,
             weekendDays: Array[Int],
             htlcd: String): (Int, Option[Array[Int]]) = {
    if (seasonWeekTagType == "sameTag")
      (1, None)
    else {
      val lvdtNum = lvdt.toString("yyyyMMdd").toInt
      val lvdtStr = lvdt.toString("yyyy-MM-dd")
      val weekDay = lvdt.getDayOfWeek
      implicit val ordrMinSeason = Ordering.by {
        (r: Array[String]) =>
          val start = r(seasonColIdx("start_dt")).replaceAll("-", "").toInt
          val end = r(seasonColIdx("end_dt")).replaceAll("-", "").toInt
          end - start
      }

      val season = seasonRows.filter {
        r =>
          val htl = r(seasonColIdx("htl_cd"))
          val startDt = r(seasonColIdx("start_dt"))
          val endDt = r(seasonColIdx("end_dt"))
          val startNum = startDt.replaceAll("-", "").toInt
          val endNum = endDt.replaceAll("-", "").toInt
          htl == htlcd && lvdtNum >= startNum && lvdtNum <= endNum
      }

      if (season.isEmpty) {
        val msg = s"Cannot find season of $htlcd and $lvdtStr"
        println(msg)
      }

      val minSeason = season.min(ordrMinSeason)
      val special = specialDaysRows.filter {
        r =>
          lvdtStr == r(holidaysColIdx("date"))
      }

      val seasonNum = minSeason(seasonColIdx("season")).toInt
      val seasonWeekTag = seasonWeekTagType match {
        case "weekendOrNot" => seasonWeekday2Tag(seasonNum, weekDay, weekendDays)
        case "noWeekday" => seasonWeekday2Tag(seasonNum)
        case _ => seasonWeekday2Tag(seasonNum, weekDay)
      }

      val specialTag = if (special.isEmpty) None else Some(special.map(r => r(holidaysColIdx("tag")).toInt))
      (seasonWeekTag, specialTag)
    }

  }


  private def seasonColIdx(colName: String) = colName match {
    case "htl_cd" => 0
    case "season" => 1
    case "start_dt" => 5
    case "end_dt" => 6
  }

  private def holidaysColIdx(colName: String) = colName match {
    case "date" => 1
    case "tag" => 2

  }
  private def hotelColIdx(colName: String) = colName match {
    case "htl_cd" => 1
  }
}