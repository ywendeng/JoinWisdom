package cn.jw.rms.ab.etl.fittotal

import cn.jw.rms.data.framework.common.rules.CleanerAssembly
import cn.jw.rms.data.framework.common.utils.HDFSUtil
import com.typesafe.config.Config
import org.apache.log4j.Logger
import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.rdd.RDD
import org.joda.time.{DateTime, Days}

import scala.reflect.io.Path
import scala.util.{Failure, Success, Try}



object SeriLogger extends Serializable {
  @transient lazy val logger = Logger.getLogger(getClass.getName)
}

class Cleaner extends CleanerAssembly with Serializable {

  import SeriLogger.logger
  var gotSucceed = false
  var errMsg = ""

  override def succeed: (Boolean, String) = (gotSucceed, errMsg)

  override def clean(sc: SparkContext, config: Config, prevStepRDD: Option[RDD[String]]): Option[RDD[String]] = {
    Try {
      println("ETL fit-total started")
      val segbkdailysumDir = config.getString("segbkdailysum-dir")
      val segpredconfDir = config.getString("predconf-dir")
      val fieldSplitter = config.getString("field-splitter")
      val distDir = config.getString("dist-dir")
      val pairRDDReparNum = config.getInt("pairRDD-repartition-num")
      val RDDReparNum = config.getInt("RDD-repartition-num")
      val fcDaysCount = config.getInt("fc-days")
      val commonHtlcd = config.getString("common-htlcd")
      val hotelDir = config.getString("hotel-dir")
      val fcHtl = config.getString("fc-htlcd")
      val saveToFile = config.getBoolean("save-result-to-file")
      val hadoopHost = config.getString("hadoop-host")
      val testOpen = config.getBoolean("hist-enable")
      val testSegbkdailysumDir = config.getString("hist-segbkdailysum-dir")
      val histStartStr = config.getString("hist-start")
      val histEndStr = config.getString("hist-end")
      val defaultHistDays = config.getInt("default-hist-days")
      val etlType = config.getString("etl-type")
      val fcDateConfigStr = config.getString("fc-date")
      val fcDatePair =
        if (fcDateConfigStr == "now") {
          val dt = new DateTime(DateTime.now().toString("yyyy-MM-dd"))/*.minusDays(1)*/
          (dt, dt)
        } else {
          val dts = fcDateConfigStr.split(",")
          val startDt = new DateTime(dts(0))/*.minusDays(1)*/
          val endDt = if (dts.length == 2) new DateTime(dts(1)) else startDt
          (startDt, endDt)
        }

      val hotelRaw = sc.textFile(hotelDir).filter(_.nonEmpty).map(_.split(fieldSplitter, -1)).filter {
        r =>
          if (fcHtl == "all")
            r(hotelColIdx("htl_cd")) != commonHtlcd
          else {
            val htlList = fcHtl.split(",")
            htlList.contains(r(hotelColIdx("htl_cd")))
          }
      }.map{
        r =>
          (r(hotelColIdx("htl_cd")), 1)
      }

      val parHotelRaw = if (pairRDDReparNum > 0) hotelRaw.partitionBy(new HashPartitioner(pairRDDReparNum)) else hotelRaw

      //val hotelCds = hotelRaw.map(_ (hotelColIdx("htl_cd")))
      val segConfRaw = sc.textFile(segpredconfDir).filter(_.nonEmpty).map(_.split(fieldSplitter, -1))
      val predSegList = segConfRaw.filter {
        r =>
          r(segPredConfColIdx("is_predict")) == "Y"
      }.map(r => (r(segPredConfColIdx("htl_cd")), r(segPredConfColIdx("seg_cd")))).distinct()

      val parPredSegList = if (pairRDDReparNum > 0) predSegList.partitionBy(new HashPartitioner(pairRDDReparNum)) else predSegList

      val joinParPredSegList = parHotelRaw.join(parPredSegList).mapPartitions{
        par =>
          par.map{
            case (htlcd, (n, segcd)) =>
              (htlcd, segcd)
          }
      }

      val startDt = fcDatePair._1
      val endDt = fcDatePair._2
      val fcDays = Days.daysBetween(startDt, endDt).getDays

      if(testOpen){
        val res = {
          val entireStartDt = new DateTime(histStartStr)
          val entireEndDt = endDt.plusDays(fcDaysCount)
          val entireStartDtNum = entireStartDt.toString("yyyyMMdd").toInt
          val entireEndDtNUm = entireEndDt.toString("yyyyMMdd").toInt
          logger.info(s"entireStartDt = $entireStartDtNum, entireEndDt = $entireEndDtNUm")
          val rawData = if (RDDReparNum > 0)
            sc.textFile(testSegbkdailysumDir).filter(_.nonEmpty).map(_.split(fieldSplitter, -1)).repartition(RDDReparNum)
          else
            sc.textFile(testSegbkdailysumDir).filter(_.nonEmpty).map(_.split(fieldSplitter, -1))

          process(rawData, joinParPredSegList, entireStartDtNum, entireEndDtNUm, pairRDDReparNum)
        }

        if (saveToFile) {
          val distPath = distDir
          if (distDir.startsWith("hdfs://")) {
            HDFSUtil.delete(hadoopHost, distPath)
          } else {
            val path: Path = Path(distPath)
            path.deleteRecursively()
          }
          res.saveAsTextFile(distPath)
        }
      } else {
        (0 to fcDays).foreach {
          day =>
            val fcDate = if(etlType == "hourly") startDt.plusDays(day) else startDt.minusDays(1).plusDays(day)
            val fcDateSimpStr = fcDate.toString("yyyyMMdd")
            val res = {
              val entireStartDt = fcDate.minusDays(defaultHistDays)
              val entireEndDt = fcDate.plusDays(fcDaysCount)
              val entireStartDtNum = entireStartDt.toString("yyyyMMdd").toInt
              val entireEndDtNUm = entireEndDt.toString("yyyyMMdd").toInt
              val path = segbkdailysumDir + "/dt=" + fcDateSimpStr
              val rawData = if (RDDReparNum > 0)
                sc.textFile(path).filter(_.nonEmpty).map(_.split(fieldSplitter, -1)).repartition(RDDReparNum)
              else
                sc.textFile(path).filter(_.nonEmpty).map(_.split(fieldSplitter, -1))
              process(rawData, joinParPredSegList, entireStartDtNum, entireEndDtNUm, pairRDDReparNum)
            }

            if (saveToFile) {
              val distPath = distDir + s"/dt=$fcDateSimpStr"
              if (distDir.startsWith("hdfs://")) {
                HDFSUtil.delete(hadoopHost, distPath)
              } else {
                val path: Path = Path(distPath)
                path.deleteRecursively()
              }
              res.saveAsTextFile(distPath)
            }
        }
      }

      println("ETL fit-total finished")
    } match {
      case Success(res) =>
        gotSucceed = true
        None
      case Failure(e) =>
        gotSucceed = false
        errMsg = e.toString
        logger.error(errMsg)
        println(errMsg)
        e.printStackTrace()
        None
    }
  }

  private def process(rawData: RDD[Array[String]], predSegList: RDD[(String, String)], startDtNum: Int, endDtNum: Int, pairRDDReparNum: Int) = {
    val filteredRawData = rawData.filter{
      r =>
        val htlcd = r(segBkDailySumColIdx("htl_cd"))
        val lvdt = r(segBkDailySumColIdx("live_dt"))
        val lvdtNum = lvdt.replaceAll("-","").toInt
        lvdtNum >= startDtNum && lvdtNum <= endDtNum
    }.mapPartitions{
      par =>
        par.map{
          r =>
            val htlcd = r(segBkDailySumColIdx("htl_cd"))
            val segcd = r(segBkDailySumColIdx("seg_cd"))
            val isMember = r(segBkDailySumColIdx("is_member"))
            val lvdt = r(segBkDailySumColIdx("live_dt"))
            val advbkdays = r(segBkDailySumColIdx("adv_bk_days"))
            val rns = r(segBkDailySumColIdx("rns"))
            val rev = r(segBkDailySumColIdx("rev"))
            (htlcd, (segcd, isMember, lvdt, advbkdays, rns, rev))
        }
    }

    val parFilteredRawData = if (pairRDDReparNum > 0) filteredRawData.partitionBy(new HashPartitioner(pairRDDReparNum)) else filteredRawData

    val segcdFit = "FIT_TOTAL"
    val predData = predSegList.join(parFilteredRawData).filter {
      case (_, (predSegcd, (segcd, _, _, _, _, _))) =>
        predSegcd == segcd
    }.mapPartitions {
      par =>
        par.map {
          case (htlcd, (_, (_, isMember, lvdt, advbkdays, rns, rev))) =>
            ((htlcd, isMember, lvdt, advbkdays), (rns.toFloat, rev.toFloat))
        }
    }.reduceByKey{
      case ((rns1, rev1),(rns2, rev2)) =>
        (rns1 + rns2, rev1 + rev2)
    }.mapPartitions{
      par =>
        par.map{
          case ((htlcd, isMember, lvdt, advbkdays), (rns, rev)) =>
            s"$htlcd#$segcdFit#$isMember#$lvdt#$advbkdays#$rns#$rev"
        }
    }

    /*val notPredData = filteredRawData.filter{
      r =>
        val htlcd = r(segBkDailySumColIdx("htl_cd"))
        val segcd = r(segBkDailySumColIdx("seg_cd"))
        !predSegList.contains((htlcd, segcd))
    }.mapPartitions{
      par =>
        par.map{
          r =>
            val htlcd = r(segBkDailySumColIdx("htl_cd"))
            val segcd = r(segBkDailySumColIdx("seg_cd"))
            val isMember = r(segBkDailySumColIdx("is_member"))
            val lvdt = r(segBkDailySumColIdx("live_dt"))
            val advbkdays = r(segBkDailySumColIdx("adv_bk_days"))
            val rns = r(segBkDailySumColIdx("rns"))
            val rev = r(segBkDailySumColIdx("rev"))
            s"$htlcd#$segcd#$isMember#$lvdt#$advbkdays#$rns#$rev"
        }
    }
    predData.union(notPredData)*/
    predData
  }

  def segBkDailySumColIdx(colName: String) = colName match {
    case "htl_cd" => 0
    case "seg_cd" => 1
    case "is_member" => 2
    case "live_dt" => 3
    case "adv_bk_days" => 4
    case "rns" => 5
    case "rev" => 6
  }

  def hotelColIdx(colName: String) = colName match {
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

  def segPredConfColIdx(colName: String) = colName match {
    case "id" => 0
    case "htl_cd" => 1
    case "seg_cd" => 2
    case "dt_tag" => 3
    case "adv_fc_days" => 4
    case "is_predict" => 5
    case "predit_para_m" => 6
    case "predit_idx_w" => 7
    case "comb_idx" => 8
  }
}
