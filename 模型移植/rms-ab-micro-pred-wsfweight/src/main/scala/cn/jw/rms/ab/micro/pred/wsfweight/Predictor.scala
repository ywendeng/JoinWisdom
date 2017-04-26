package cn.jw.rms.ab.micro.pred.wsfweight

import cn.jw.rms.data.framework.common.rules.PredictModelAssembly
import cn.jw.rms.data.framework.common.utils.HDFSUtil
import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.joda.time.{DateTime, Days}

import scala.collection.Map
import scala.reflect.io.Path
import scala.util.{Failure, Success, Try}


class Predictor extends PredictModelAssembly with Serializable {

  var gotSucceed = false
  var errMsg = ""
  val DATE_FORMAT_STR = "yyyy-MM-dd"
  val DT_FORMAT_STR = "yyyyMMdd"

  override def accuracy(sc: SparkContext, config: Config, prevStepRDD: Option[RDD[String]]): String = "Not Support!"

  override def succeed: (Boolean, String) = (gotSucceed, errMsg)

  override def predict(sc: SparkContext, config: Config, prevStepRDD: Option[RDD[String]]): Option[RDD[String]] = {
    Try {
      println("wsfweight started")
      val testOpen = config.getBoolean("hist-enable")
      val saveToFile = config.getBoolean("save-result-to-file")
      val fc_date = config.getString("fc-date") match {
        case "now" => new DateTime().minusDays(1).toString(DATE_FORMAT_STR)
        case _ => new DateTime(config.getString("fc-date")).minusDays(1).toString(DATE_FORMAT_STR)
      }
      println(s"fc_date = $fc_date")
      val hadoop_host = config.getString("hadoop-host")
      val wsf_dist_dir = if (testOpen) config.getString("wsf-dist-dir") else addDt(config.getString("wsf-dist-dir"), fc_date)
      val conf_dist_dir =  config.getString("conf-dist-dir")
      val dayTagDir = config.getString("datetag-dir")
      val field_splitter = config.getString("field-splitter")
      val RDD_repartition_num = config.getString("RDD-repartition-num")
      val advFcDays: Int = config.getInt("adv-fc-days")
      val segType = config.getString("seg-type")
      val exclude_livedt = config.getString("exclude-livedt").split(",")
      val fcHtlcdStr = config.getString("fc-htlcd")
      val fcHtlcdList = fcHtlcdStr.split(",")
      /**
        * segbkdailysum细分市场的数据
        */
      val daily_input_path = if (testOpen) config.getString("hist-segbkdailysum-dir") else addDt(config.getString("segbkdailysum-dir"), fc_date)
      /*
      val longterm_input_path = if (testOpen) config.getString("hist-longterm-dir") else addDt(config.getString("longterm-sum-dir"), fc_date)
      val shortterm_input_path = if (testOpen) config.getString("hist-shortterm-dir") else addDt(config.getString("shortterm-sum-dir"), fc_date)
      */
      val wsfHistDaysOpen = config.getBoolean("wsf-hist-days-open")
      /**
        * longterm中的数据
        */
      val longterm_input_path = if (testOpen) config.getString("hist-longterm-dir") else {
        if(wsfHistDaysOpen)
          config.getString("longterm-sum-dir")
        else
          addDt(config.getString("longterm-sum-dir"),fc_date)
      }
      /**
        * shorterm中的数据
        */
      val shortterm_input_path = if (testOpen) config.getString("hist-shortterm-dir") else {
        if(wsfHistDaysOpen)
          config.getString("shortterm-sum-dir")
        else
          addDt(config.getString("shortterm-sum-dir"),fc_date)
      }
      /**
        * 短期预测中的权重值
        * 4#5#0.3
          5#5#0.2
          6#7#0.25
          7#4#0.275
        */
      val shortterm_mw_input_path = config.getString("shortterm-mw-dir")
      /**
        * 预测配置表
        */
      val ispred_path = config.getString("ispred-dir")
      val hotelDir = config.getString("hotel-dir")
      val commonHtlcd = config.getString("common-htlcd")
      val startDtStr = config.getString("start-dt")
      val endDtStr = config.getString("end-dt")

      /**
        * 获取训练日期的开始时间和结束时间
        */
      val (startDt, endDt) = if (wsfHistDaysOpen) {
        val wsfHistDays = config.getInt("wsf-hist-days")
        val newEndDt = new DateTime(fc_date)
        val newStarDt = newEndDt.minusDays(wsfHistDays)
        (newStarDt.toString("yyyy-MM-dd"), newEndDt.toString("yyyy-MM-dd"))
      } else (startDtStr, endDtStr)
      println(s"startDt = $startDt,endDt = $endDt")
      val (starDtNum, endDtNum) = {
        (startDt.replace("-", "").toInt, endDt.replace("-", "").toInt)
      }

      val dayTagSet: Map[(String, String), String] = if (dayTagDir.nonEmpty) {
        /**
          * 获取dayTagData数据集
         */
       val dayTagData =  sc.textFile(dayTagDir).map(_.split(field_splitter))
        dayTagData.map{
          lines=>
            val htl_cd  = lines(ColIdxDayTag("htl_cd"))
            val live_dt = lines(ColIdxDayTag("live_dt"))
            val day_tag = lines(ColIdxDayTag("day_tag"))
            ((htl_cd,live_dt),day_tag)
        }.collectAsMap()
      } else  Map(("any","any")->"0")

      val hotelRaw = sc.textFile(hotelDir).filter(_.nonEmpty).map(_.split(field_splitter, -1)).filter {
        r =>
          if (fcHtlcdStr == "all")
            r(hotelColIdx("htl_cd")) != commonHtlcd
          else {
            fcHtlcdList.contains(r(hotelColIdx("htl_cd")))
          }
      }.collect()
      val htlcdList = hotelRaw.map(_ (hotelColIdx("htl_cd")))

      val isPredConfig = sc.textFile(ispred_path).map(_.split(field_splitter, -1))

      val mwMap = sc.textFile(shortterm_mw_input_path).map(_.split(field_splitter, -1)).map { fields =>
        val adv_fc_days = fields(0)
        val short_m = fields(1)
        val short_w = fields(2)

        /**
          * 6#7#0.25------提前预定天数#short_m#short_w
          */
        (adv_fc_days, (short_m, short_w))
      }.collectAsMap()

      val rawActual = sc.textFile(daily_input_path).map(_.split(field_splitter, -1)).filter {
        r =>
          val htlcd = r(colIndexDailySum("htl_cd"))
          val liveDt = r(colIndexDailySum("live_dt"))
          val liveDtNum = liveDt.replace("-", "").toInt
          htlcdList.contains(htlcd) &&
            liveDtNum >= starDtNum &&
            liveDtNum <= endDtNum &&
            // should improve exclude_livedt
            !exclude_livedt.contains(liveDt)
      }

      val rawLongterm = sc.textFile(longterm_input_path).map(_.split(field_splitter, -1)).filter {
        r =>
          val htlcd = r(colIndexForcastDetail("htl_cd"))
          val paraTyp = r(colIndexForcastDetail("para_typ"))
          val liveDt = r(colIndexForcastDetail("live_dt"))
          val liveDtNum = liveDt.replace("-", "").toInt
          val orderDt = r(colIndexForcastDetail("order_dt"))
          val fcDt = r(colIndexForcastDetail("fc_dt"))
          val advDays = Days.daysBetween(new DateTime(fcDt), new DateTime(liveDt)).getDays

          orderDt == liveDt && "1" == paraTyp && advDays <= advFcDays &&
            htlcdList.contains(htlcd) &&
            liveDtNum >= starDtNum &&
            liveDtNum <= endDtNum &&
            // should improve exclude_livedt
            !exclude_livedt.contains(liveDt)
      }
//      rawLongterm.take(10).foreach(println)
      val rawShortterm = sc.textFile(shortterm_input_path).map(_.split(field_splitter, -1)).filter {
        r =>
          val htlcd = r(colIndexForcastDetail("htl_cd"))
          val paraTyp = r(colIndexForcastDetail("para_typ"))
          val liveDt = r(colIndexForcastDetail("live_dt"))
          val liveDtNum = liveDt.replace("-", "").toInt
          val orderDt = r(colIndexForcastDetail("order_dt"))
          val fcDt = r(colIndexForcastDetail("fc_dt"))
          val advDays = Days.daysBetween(new DateTime(fcDt), new DateTime(liveDt)).getDays

          orderDt == liveDt && "1" == paraTyp && advDays <= advFcDays &&
            htlcdList.contains(htlcd) &&
            liveDtNum >= starDtNum &&
            liveDtNum <= endDtNum &&
            // should improve exclude_livedt
            !exclude_livedt.contains(liveDt)
      }


      htlcdList.foreach {
        htl =>
          val htlActual = rawActual.filter {
            r =>
              val htlcd = r(colIndexDailySum("htl_cd"))
              htl == htlcd
          }

          val htlLongterm = rawLongterm.filter {
            r =>
              val htlcd = r(colIndexForcastDetail("htl_cd"))
              htl == htlcd
          }

          val htlShortterm = rawShortterm.filter {
            r =>
              val htlcd = r(colIndexForcastDetail("htl_cd"))
              htl == htlcd
          }
          val isPredHtlConfig = isPredConfig.filter{
            r =>
              val htlcd = r(ColIdxIsPred("htl_cd"))
              htl == htlcd
          }
          /**
            * 根据actual,longterm,shorterm,advFcDays(提前预定天数)
            * dayTagSet计算longterm,shorterm对应的权重
            *
            */
          val htlWsf = getWsf(htlActual, htlLongterm, htlShortterm, segType, advFcDays,dayTagSet).cache()
//          htlWsf.take(30).foreach(println)
          val htlConf = getConf(isPredHtlConfig, mwMap, htlWsf, segType, advFcDays, dayTagSet, field_splitter)
//          htlConf.take(30).foreach(println)
          val newWsf = if (!RDD_repartition_num.equals("-1")) {
            htlWsf.repartition(RDD_repartition_num.toInt)
          } else htlWsf

          val newConf = if (!RDD_repartition_num.equals("-1")) {
            htlConf.repartition(RDD_repartition_num.toInt)
          } else htlConf

          if (saveToFile) {
            val wsfDistPath = s"$wsf_dist_dir/$htl"
            val confDistPath = s"$conf_dist_dir/$htl"
            if (!hadoop_host.equals("local")) {
              HDFSUtil.delete(hadoop_host, wsfDistPath)
              HDFSUtil.delete(hadoop_host, confDistPath)
            } else {
              val wsfPath: Path = Path(wsfDistPath)
              wsfPath.deleteRecursively()
              val confPath: Path = Path(confDistPath)
              confPath.deleteRecursively()
            }
            newWsf.saveAsTextFile(wsfDistPath)
            newConf.saveAsTextFile(confDistPath)
          }
      }
      println("wsfweight finished")
      None
    } match {
      case Success(result) =>
        gotSucceed = true
        None
      case Failure(e) =>
        errMsg = e.getMessage
        println(errMsg)
        e.printStackTrace()
        None
    }
  }

  /**
    *
    */

  private def getWsf(actualData: RDD[Array[String]], longtermData: RDD[Array[String]], shorttermData: RDD[Array[String]],
                     segType: String, advFcDays: Int, dayTagSet:Map[(String, String), String]) = {
    /**
      * 把所有日期对应的相同(htl_cd, seg_cd, live_dt)的间夜量相加
      */
    val actualSum = actualData.map { fields =>
      val htl_cd = fields(colIndexDailySum("htl_cd"))
      val seg_cd = fields(colIndexDailySum("seg_cd"))
      val live_dt = fields(colIndexDailySum("live_dt"))
      val rns = fields(colIndexDailySum("rns")).toFloat
      ((htl_cd, seg_cd, live_dt), rns)
    }.reduceByKey {
      (x, y) => x + y
    }.flatMap { case (k, v) =>
      val htl_cd = k._1
      val seg_cd = if (segType =="FIT_TOTAL") segType else k._2
      val live_dt = k._3
      for (adv_fc <- 1 to advFcDays) yield {
        ((htl_cd, seg_cd, live_dt, adv_fc), v)
      }
    }

    val longtermSum = longtermData.map { fields =>
      val htl_cd = fields(colIndexForcastDetail("htl_cd"))
      val para_cd = if (segType =="FIT_TOTAL") segType else fields(colIndexForcastDetail("para_cd"))
      val live_dt = fields(colIndexForcastDetail("live_dt"))
      val fc_dt = fields(colIndexForcastDetail("fc_dt"))
      val fc_occ = fields(colIndexForcastDetail("fc_occ")).toFloat
      val adv_bk_days = Days.daysBetween(new DateTime(fc_dt), new DateTime(live_dt)).getDays
      ((htl_cd, para_cd, live_dt, adv_bk_days), fc_occ)
    }.reduceByKey { (x, y) => x + y }

    val shorttermSum = shorttermData.map { fields =>
      val htl_cd = fields(colIndexForcastDetail("htl_cd"))
      val para_cd = if (segType =="FIT_TOTAL") segType else fields(colIndexForcastDetail("para_cd"))
      val live_dt = fields(colIndexForcastDetail("live_dt"))
      val fc_dt = fields(colIndexForcastDetail("fc_dt"))
      val fc_occ = fields(colIndexForcastDetail("fc_occ")).toFloat
      val adv_bk_days = Days.daysBetween(new DateTime(fc_dt), new DateTime(live_dt)).getDays
      ((htl_cd, para_cd, live_dt, adv_bk_days), fc_occ)
    }.reduceByKey { (x, y) => x + y }
    /**
      * 使用actual和shorterm,lonterm 进行连接操作
      */
    val join:RDD[((String, String, String, Int), ((Float, Option[Float]), Option[Float]))] = actualSum.leftOuterJoin(longtermSum).leftOuterJoin(shorttermSum)

    val base = join.map { case (key, v) =>
      val htl_cd = key._1
      val seg_cd = key._2
      val live_dt = key._3
      val day_tag = if (dayTagSet.contains (htl_cd,live_dt)) dayTagSet((htl_cd,live_dt)) else  0
      val adv_bk_days = key._4
      val rns = v._1._1
      val fc_occ_long = if (v._1._2.isDefined) v._1._2.get else 0
      val fc_occ_short = if (v._2.isDefined) v._2.get else 0


      val p = fc_occ_long * fc_occ_long
      val q = fc_occ_long * fc_occ_short
      val r = fc_occ_long * fc_occ_short
      val s = fc_occ_short * fc_occ_short
      val k = fc_occ_long * rns
      val h = fc_occ_short * rns
      ((htl_cd, seg_cd, adv_bk_days, day_tag), (p, q, r, s, k, h))

    }.reduceByKey { (x, y) =>
      (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4, x._5 + y._5, x._6 + y._6)
    }
    base.map { case (key, value) =>
      val htl_cd = key._1
      val seg_cd = key._2
      val adv_bk_days = key._3
      val day_tag = key._4
      val p = value._1
      val q = value._2
      val r = value._3
      val s = value._4
      val k = value._5
      val h = value._6
      /*
      val lt_wsf = if (p * s - r * q == 0) {
        if (q == 0) {
          1
        } else {
          k / q
        }
      } else {
        (k * s - h * q) / (p * s - r * q)
      }
      */
      val lt_wsf = if (p * s - r * q == 0) {
        if (p == 0) {
          0
        } else if (s == 0) {
          1
        } else {
          0.5
        }
      } else {
        (k * s - h * q) / (p * s - r * q)
      }

      /*
      val st_wsf = if (p * s - r * q == 0) {
        0
      } else {
        (h * p - k * r) / (p * s - r * q)
      }
      */
      val st_wsf = if (p * s - r * q == 0) {
        if (p == 0) {
          1
        } else if (s == 0) {
          0
        } else {
          0.5
        }
      } else {
        (h * p - k * r) / (p * s - r * q)
      }
      s"$htl_cd#$seg_cd#$adv_bk_days#$day_tag#$lt_wsf#$st_wsf"
    }
  }

  private def getConf(isPredHtlConfig: RDD[Array[String]], mwMap: collection.Map[String, (String, String)], wsf: RDD[String],
                      segType: String, advFcDays: Int, dayTagSet:Map[(String, String), String], field_splitter: String) = {
    //create big table
    val entireTable = isPredHtlConfig.flatMap {
      fields =>
        val htlCd = fields(ColIdxIsPred("htl_cd"))
        val segCd = if (segType == "FIT_TOTAL") segType else fields(ColIdxIsPred("seg_cd"))
        val isPred = if (segType == "FIT_TOTAL") "Y" else fields(ColIdxIsPred("is_pred"))
        (-1 to advFcDays).flatMap {
          day =>
            dayTagSet.map {
              tag =>
                val day_tag= tag._2
                ((htlCd, segCd, day, day_tag), isPred)
            }
        }
    }.distinct()
//    entireTable.take(10).foreach(println)
    val confData = wsf.flatMap { line =>
      val fields = line.split(field_splitter)
      val htl_cd = fields(0)
      val seg_cd = fields(1)
      val adv_bk_days = fields(2)
      val dayTag = fields(3)
      val lt_wsf = fields(4)
      val st_wsf = fields(5)
      val predit_para_m = if (mwMap.get(adv_bk_days).isDefined) mwMap.get(adv_bk_days).get._1 else mwMap.get("7").get._1
      val predit_para_w = if (mwMap.get(adv_bk_days).isDefined) mwMap.get(adv_bk_days).get._2 else mwMap.get("7").get._2
      val comb_idx = lt_wsf + "," + st_wsf

      if (advFcDays.equals(adv_bk_days.toInt)) {
        for (a <- Array(-1, advFcDays)) yield {
          s"1#$htl_cd#$seg_cd#$dayTag#$a#$predit_para_m#$predit_para_w#$comb_idx"
        }
      } else {
        for (a <- Array(adv_bk_days)) yield {
          s"1#$htl_cd#$seg_cd#$dayTag#$a#$predit_para_m#$predit_para_w#$comb_idx"
        }
      }
    }.map {
      lines =>
        val fields = lines.split(field_splitter)
        val htl_cd = fields(1)
        val seg_cd = fields(2)
        val day_tag = fields(3)
        val adv_bk_day = fields(4).toInt
        val predit_para_m = fields(5)
        val predit_para_w = fields(6)
        val comb_idx = fields(7)
        ((htl_cd, seg_cd, adv_bk_day, day_tag), (predit_para_m, predit_para_w, comb_idx))
    }

    entireTable.leftOuterJoin(confData).map {
      case (key, (a, b)) =>
        val value = b.getOrElse("4", "0.275", "1.0,0.0")
        val is_pred = a
        val htl_cd = key._1
        val seg_cd = key._2
        val day_tag = key._4
        val adv_bk_day = key._3
        val predit_para_m = value._1
        val predit_para_w = value._2
        val comb_idx = value._3
        s"1#$htl_cd#$seg_cd#$day_tag#$adv_bk_day#$is_pred#$predit_para_m#$predit_para_w#$comb_idx"
    }.sortBy {
      x =>
        val fields = x.split(field_splitter)
        val htl_cd = fields(1)
        val seg_cd = fields(2)
        val adv_bk_days = fields(4)
        s"$htl_cd$seg_cd$adv_bk_days"
    }
  }

  private def ColIdxIsPred(colName: String): Int = colName match {
    case "htl_cd" => 0
    case "seg_cd" => 1
    case "is_pred" => 2
  }
  private def ColIdxDayTag(colName: String): Int = colName match {
    case "htl_cd" => 0
    case "live_dt" => 1
    case "day_tag" => 2
  }
  def colIndexDailySum(colname: String): Int = colname match {
    case "htl_cd" => 0
    case "seg_cd" => 1
    case "is_member" => 2
    case "live_dt" => 3
    case "adv_bk_days" => 4
    case "rns" => 5
    case "rev" => 6
  }

  def colIndexForcastDetail(colname: String): Int = colname match {
    case "htl_cd" => 0
    case "para_typ" => 1
    case "para_cd" => 2
    case "order_dt" => 3
    case "live_dt" => 4
    case "fc_dt" => 5
    case "fc_occ" => 6
    case "fc_rev" => 7
    case "update_dt" => 8
  }

  private def hotelColIdx(colName: String) = colName match {
    case "id" => 0
    case "htl_cd" => 1
    case "ctrip_hotelid" => 2
    case "htl_nm" => 3
    case "htl_nm_en" => 4
    case "nation_id" => 5
    case "city_id" => 6
    case "zip" => 7
    case "tel" => 8
    case "fax" => 9
    case "addr" => 10
    case "htl_company" => 11
    case "pms_cd" => 12
    case "pms_version" => 13
    case "currency" => 14
    case "currencydes" => 15
    case "rns" => 16
    case "int_type" => 17
    case "int_version" => 18
    case "first_dt" => 19
    case "future_days" => 20
    case "gen_process" => 21
    case "options" => 22
    case "update_time" => 23
  }

  private def addDt(path: String, fc_date: String): String = {
    if (path.endsWith("/")) {
      path + "dt=" + new DateTime(fc_date).toString(DT_FORMAT_STR)
    } else {
      path + "/dt=" + new DateTime(fc_date).toString(DT_FORMAT_STR)
    }

  }
}
