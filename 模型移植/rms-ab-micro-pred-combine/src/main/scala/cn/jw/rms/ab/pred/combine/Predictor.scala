package cn.jw.rms.ab.pred.combine

import cn.jw.rms.data.framework.common.rules.PredictModelAssembly
import cn.jw.rms.data.framework.common.utils.HDFSUtil
import com.typesafe.config.Config
import org.apache.log4j.Logger
import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.rdd.RDD
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, Days}
import scala.reflect.io.Path
import scala.util.{Failure, Success, Try}
import collection.JavaConversions._




object SeriLogger extends Serializable {
  @transient lazy val logger = Logger.getLogger(getClass.getName)
}

class Predictor extends PredictModelAssembly with Serializable {

  import SeriLogger.logger

  var gotSucceed = false
  var errMsg = ""

  override def accuracy(sc: SparkContext, config: Config, prevStepRDD: Option[RDD[String]]): String = "Not Supported"

  override def succeed: (Boolean, String) = (gotSucceed, errMsg)

  override def predict(sc: SparkContext, config: Config, prevStepRDD: Option[RDD[String]]): Option[RDD[String]] = {
    Try {
      println("Combine started")
      val longtermDir = config.getString("longterm-dir")
      val shorttermDir = config.getString("shortterm-dir")
      val segpredconfDir = config.getString("predconf-dir")
      val seasonDir = config.getString("season-dir")
      val holidayDir = config.getString("specialdays-dir")
      val fieldSplitter = config.getString("field-splitter")
      val distDir = config.getString("dist-dir")
      val pairRDDReparNum = config.getInt("pairRDD-repartition-num")
      val RDDReparNum = config.getInt("RDD-repartition-num")
      val fcDayCount = config.getInt("fc-days")
      val commonHtlcd = config.getString("common-htlcd")
      val hotelDir = config.getString("hotel-dir")
      val fcHtl = config.getString("fc-htlcd")
      val saveToFile = config.getBoolean("save-result-to-file")
      val hadoopHost = config.getString("hadoop-host")
      val testOpen = config.getBoolean("hist-enable")
      val seasonWeekTagType = config.getString("season-weekday-tag-type")
      val weekendDays = config.getIntList("weekend-days").map(_.toInt).toArray
      val matrixCols = config.getIntList("matrix-columns").map(_.toInt).toArray
      val segType = config.getString("seg-type")
      val fcDateStr = config.getString("fc-date")
      val specialDaysTagEnable = config.getBoolean("special-days-tag-enable")
      val segbkdailysumDir = config.getString("segbkdailysum-dir")
      val faultTolerance = config.getBoolean("fault-tolerance")
      val advIndex =config.getInt("adv-index")
      val fcDatePair =
        if (fcDateStr == "now") {
          val dt = new DateTime(DateTime.now().toString("yyyy-MM-dd")).minusDays(1)
          (dt, dt)
        } else {
          val dts = fcDateStr.split(",")
          val startDt = new DateTime(dts(0)).minusDays(1)
          val endDt = if (dts.length == 2) new DateTime(dts(1)) else startDt
          (startDt, endDt)
        }

      // get start adv days/hours and end adv days/hours of each column
      val origMatCols = if (matrixCols(0) > 0) 0 +: matrixCols else matrixCols
      val matrixIdx2ColsMap = origMatCols.indices.map {
        idx =>
          val endIdx = if (idx > origMatCols.length - 1) origMatCols.length - 1 else idx + 2
          val g = origMatCols.slice(idx, endIdx)
          val startNum = g(0)

          val endNum =
            if (g.length > 1) g(1) - 1 else g(0)
          (idx, (startNum, endNum))

      }.sortBy(_._1)

      matrixIdx2ColsMap.foreach(logger.info)

      val MAT_COL_LEN = matrixIdx2ColsMap.size

      // get hotel list
      val hotelRaw = sc.textFile(hotelDir).filter(_.nonEmpty).map(_.split(fieldSplitter, -1)).filter {
        r =>
          if (fcHtl == "all")
            r(hotelColIdx("htl_cd")) != commonHtlcd
          else {
            val htlList = fcHtl.split(",")
            htlList.contains(r(hotelColIdx("htl_cd")))
          }
      }.collect()

      val hotelCds = hotelRaw.map(_ (hotelColIdx("htl_cd")))
      //get season raw data
      val seasonRaw = sc.textFile(seasonDir).filter(_.nonEmpty).map(_.split(fieldSplitter, -1)).collect()
      //get holiday raw data
      val holidayRaw = sc.textFile(holidayDir).filter(_.nonEmpty).map(_.split(fieldSplitter, -1)).collect()
      val segConfRaw = sc.textFile(segpredconfDir).filter(_.nonEmpty).map(_.split(fieldSplitter, -1))
      val usingSegConf = segConfRaw.filter {
        r =>
          hotelCds.contains(r(segPredConfColIdx("htl_cd")))
      }.collect()

      val segConfDistinct = if (segType == "FIT_TOTAL") {
        usingSegConf.map(_ (segPredConfColIdx("htl_cd"))).distinct.map {
          htl =>
            (htl, "FIT_TOTAL")
        }
      } else {
        usingSegConf.map {
          r =>
            val htlcd = r(segPredConfColIdx("htl_cd"))
            val segcd = r(segPredConfColIdx("seg_cd"))
            (htlcd, segcd)
        }.distinct
      }

      val startDt = fcDatePair._1
      val endDt = fcDatePair._2
      val fcDays = Days.daysBetween(startDt, endDt).getDays
      val startDtStr = startDt.toString("yyyyMMdd")
      val endDtStr = endDt.plusDays(fcDayCount).toString("yyyyMMdd")
      val startDtNum = startDtStr.toInt
      val endDtNum = endDtStr.toInt
      val fcDateTags= hotelCds.flatMap {
        htl => (0 until fcDayCount + fcDays).map {
          day =>
            val dt = startDt.plusDays(day + 1)
            val tag = dt2Tag(dt, seasonRaw, holidayRaw, seasonWeekTagType, weekendDays, htl, specialDaysTagEnable)
            ((tag._1, dt.toString("yyyyMMdd").toInt), tag._2)
        }
      }.toMap

      if (testOpen) {
        process(longtermDir, shorttermDir, distDir, segbkdailysumDir)
      } else {
        (0 to fcDays).foreach {
          day =>
            val fcDate = startDt.plusDays(day)
            val fcDateSimpStr = fcDate.toString("yyyyMMdd")
            logger.info(s"fcDate = $fcDateStr")
            val partitionName = "/dt=" + fcDateSimpStr
            val longtermPath = longtermDir + partitionName
            val shorttermPath = shorttermDir + partitionName
            val distPath = distDir + partitionName
            val segbkdailysumPath = segbkdailysumDir + partitionName
            process(longtermPath, shorttermPath, distPath, segbkdailysumPath)
        }
      }

      /*
       kernel method
      */
      def process(longtermPath: String, shorttermPath: String, distPath: String, segbkdailysumPath: String) = {
        // get otb result
        val segbkdailySum = if (RDDReparNum > 0)
          sc.textFile(segbkdailysumPath).filter(_.nonEmpty).map(_.split(fieldSplitter, -1)).repartition(RDDReparNum)
        else
          sc.textFile(segbkdailysumPath).filter(_.nonEmpty).map(_.split(fieldSplitter, -1))

        val otbResult = genOrigMat(segbkdailySum, startDtStr, endDtStr, segConfDistinct, matrixIdx2ColsMap, faultTolerance)

        val otbResultWithFcdt = otbResult.mapPartitions {
          par =>
            par.flatMap {
              case ((htlcd, segcd, liveDt), row) =>
                (0 to/*until fcDayCount +*/ fcDays).map {
                  day =>
                    val fcdt = startDt.plusDays(day).toString("yyyy-MM-dd")
                    ((fcdt, htlcd, segcd, liveDt), row)
                }
            }
        }.filter{
          case ((fcdt, htlcd, segcd, liveDt), row)=>
            val fcDt = fcdt.replaceAll("-","").toInt
            val lvDt = liveDt.replaceAll("-","").toInt
            lvDt > fcDt
        }

        // get long term result
        val longtermResult = (if (RDDReparNum > 0)
          sc.textFile(longtermPath).filter(_.nonEmpty).map(_.split(fieldSplitter, -1)).repartition(RDDReparNum)
        else
          sc.textFile(longtermPath).filter(_.nonEmpty).map(_.split(fieldSplitter, -1))).cache()

        // get short term result
        val shorttermResult = (if (RDDReparNum > 0)
          sc.textFile(shorttermPath).filter(_.nonEmpty).map(_.split(fieldSplitter, -1)).repartition(RDDReparNum)
        else
          sc.textFile(shorttermPath).filter(_.nonEmpty).map(_.split(fieldSplitter, -1))).cache()

        val longtermKv = longtermResult.filter {
          r =>
            val lvdt = r(matResultColIdx("live_dt"))
            val lvdtNum = lvdt.replaceAll("-", "").toInt
            val htlcd = r(matResultColIdx("htl_cd"))
            lvdtNum >= startDtNum + 1 && lvdtNum <= endDtNum && hotelCds.contains(htlcd)
        }.mapPartitions {
          par =>
            par.map {
              r =>
                val fcdt = r(matResultColIdx("fc_dt"))
                val htlcd = r(matResultColIdx("htl_cd"))
                val segcd = r(matResultColIdx("seg_cd"))
                val lvdt = r(matResultColIdx("live_dt"))
                val values = r.slice(4, r.length).map(_.toInt)
                ((fcdt, htlcd, segcd, lvdt), values)
            }
        }
        val usingLongtermKv = if (pairRDDReparNum > 0) longtermKv.partitionBy(new HashPartitioner(pairRDDReparNum)) else longtermKv

        val shorttermKv = shorttermResult.filter {
          r =>
            val lvdt = r(matResultColIdx("live_dt"))
            val lvdtNum = lvdt.replaceAll("-", "").toInt
            lvdtNum >= startDtNum + 1 && lvdtNum <= endDtNum
        }.mapPartitions {
          par =>
            par.map {
              r =>
                val fcdt = r(matResultColIdx("fc_dt"))
                val htlcd = r(matResultColIdx("htl_cd"))
                val segcd = r(matResultColIdx("seg_cd"))
                val lvdt = r(matResultColIdx("live_dt"))
                val values = r.slice(4, r.length).map(_.toInt)
                ((fcdt, htlcd, segcd, lvdt), values)
            }
        }

        val predShorttermKv = shorttermKv.filter {
          case ((_, htlcd, segcd, _), _) =>
            segConfDistinct.contains((htlcd, segcd))
        }
        val usingPredShorttermKv = if (pairRDDReparNum > 0) predShorttermKv.partitionBy(new HashPartitioner(pairRDDReparNum)) else predShorttermKv

        val notPredShorttermKv = shorttermKv.filter {
          case ((_, htlcd, segcd, _), _) =>
            !segConfDistinct.contains((htlcd, segcd))
        }

        val usingNotPredShorttermKv = if (pairRDDReparNum > 0) notPredShorttermKv.partitionBy(new HashPartitioner(pairRDDReparNum)) else notPredShorttermKv

        val entireKeys = usingPredShorttermKv.keys.union(usingLongtermKv.keys).union(otbResultWithFcdt.keys).distinct.mapPartitions {
          par =>
            par.map {
              case (fcdtStr, htlcd, segcd, lvdtStr) =>
                ((fcdtStr, htlcd, segcd, lvdtStr), Array.emptyIntArray)
            }
        }.filter{
          case ((fcdtStr, htlcd, segcd, lvdtStr), value)=>
            val fcDt = DateTime.parse(fcdtStr,DateTimeFormat.forPattern("yyyy-MM-dd"))
            val lvDt = DateTime.parse(lvdtStr,DateTimeFormat.forPattern("yyyy-MM-dd"))
            val diffDays = Days.daysBetween(fcDt,lvDt).getDays
            diffDays <= fcDayCount && segcd.nonEmpty
        }

        val predCombineResult = entireKeys.leftOuterJoin(usingLongtermKv).leftOuterJoin(usingPredShorttermKv).leftOuterJoin(otbResultWithFcdt).mapPartitions {
          par =>
            par.map {
              case ((fcdtStr, htlcd, segcd, lvdtStr), (((_, lfOpt), sfOpt), otbOpt)) =>
                val fcdt = DateTime.parse(fcdtStr, DateTimeFormat.forPattern("yyyy-MM-dd"))
                val lvdt = DateTime.parse(lvdtStr, DateTimeFormat.forPattern("yyyy-MM-dd"))

                val lvdtNum = lvdt.toString("yyyyMMdd").toInt
                val dtTag = fcDateTags((htlcd, lvdtNum))
                val specialTag = dtTag._2
                val seasonWeekDayTag = dtTag._1
                val usingTag = if (specialTag.isDefined) specialTag.get.head else seasonWeekDayTag

                val currAdvBkDay = Days.daysBetween(fcdt, lvdt).getDays
//                println(s"fc-dt=$fcdt , lvdt=$lvdt ,  advBk=$currAdvBkDay")
                // find weight
                val currConf = usingSegConf.filter(r => r(segPredConfColIdx("htl_cd")) == htlcd && r(segPredConfColIdx("seg_cd")) == segcd)
                val predConf = currConf.filter(r => r(segPredConfColIdx("dt_tag")).toInt == usingTag && r(segPredConfColIdx("adv_fc_days")).toInt == currAdvBkDay)
                val usingConf = if (predConf.isEmpty) {
                  val conf2nd = currConf.filter(r => r(segPredConfColIdx("dt_tag")).toInt == 0 && r(segPredConfColIdx("adv_fc_days")).toInt == currAdvBkDay)
                  if (conf2nd.isEmpty) {
                    val segDefaultConf = currConf.filter(r => r(segPredConfColIdx("dt_tag")).toInt == 0 && r(segPredConfColIdx("adv_fc_days")).toInt == -1)
                    if (segDefaultConf.isEmpty) {
                      val defaultAdvFcDayW = currConf.filter(r => r(segPredConfColIdx("seg_cd")) == "FIT_TOTAL" && r(segPredConfColIdx("adv_fc_days")).toInt == currAdvBkDay)
                      if (defaultAdvFcDayW.isEmpty) {
                        val conf = currConf.filter(r => r(segPredConfColIdx("seg_cd")) == "FIT_TOTAL" && r(segPredConfColIdx("adv_fc_days")).toInt == -1)
                        if (conf.isEmpty) {
                          val msg = s"weight not found#$htlcd#$segcd#$usingTag#$currAdvBkDay"
                          println(msg)
                          logger.warn(msg)
                          Array.empty[String]
                        } else conf.head
                      } else defaultAdvFcDayW.head
                    } else segDefaultConf.head
                  } else conf2nd.head
                } else predConf.head

                // get combine weights
                val combineWeightStr = usingConf(segPredConfColIdx("comb_idx"))

                //println(s"segcd = $segcd, lvdtStr = $lvdtStr, usingTag = $usingTag, currAdvBkDay = $currAdvBkDay")
                //println(s"usingConf = ${usingConf.mkString("#")}")

                // _1: long term weight _2: short term weight
                val weighStrArr = combineWeightStr.split(",")
                val thetOpt = if (weighStrArr.length == 3) Some(weighStrArr(2).toDouble) else None
                val usingWeight = (weighStrArr(0).toDouble, weighStrArr(1).toDouble, thetOpt)

                val combResult = if (lfOpt.isDefined && sfOpt.isDefined) {
                  /*val msg = s"~1($fcdtStr, $htlcd, $segcd, $lvdtStr) when lfOpt.isDefined && sfOpt.isDefined"
                  logger.info(msg)*/
                  val lfArr= lfOpt.get
                  val sfArr = sfOpt.get
                  if (otbOpt.isDefined) {
                    val otbRow = otbOpt.get.toArray
                    val otbColIdx = advBkPeriod2ColIdx(currAdvBkDay, matrixIdx2ColsMap) // + 1
                    (0 until MAT_COL_LEN).map {
                      cIdx =>
                        val combRns = if (cIdx < otbColIdx) {
                          if (currAdvBkDay < advIndex) {
                            lfArr(cIdx) * usingWeight._1 + sfArr(cIdx) * usingWeight._2 + usingWeight._3.getOrElse(0.0)
                          }else
                            lfArr(cIdx)
                        }
                        else otbRow(cIdx)
                        if (combRns < 0) -math.round(math.abs(combRns)) else math.round(combRns)
                    }
                  } else {
                    /*val msg = s"($fcdtStr, $htlcd, $segcd, $lvdtStr) otb not found when lfOpt.isDefined && sfOpt.isDefined"
                    logger.warn(msg)*/
                    (0 until MAT_COL_LEN).map{
                      cIdx =>
                        if (cIdx < advIndex)
                        {
                          val combRns = lfArr(cIdx) * usingWeight._1 + sfArr(cIdx) * usingWeight._2 + usingWeight._3.getOrElse(0.0)
                          if (combRns < 0) -math.round(math.abs(combRns)) else math.round(combRns)
                        }
                        else
                       lfArr(cIdx)
                    }
                  }

                  /*  (0 until MAT_COL_LEN).map {
                      cIdx =>
                        val longTermRns = lfOpt.get(cIdx)
                        val shortTermRns = sfOpt.get(cIdx)
                        val combRns = longTermRns * usingWeight._1 + shortTermRns * usingWeight._2
                        if (combRns < 0) -math.round(math.abs(combRns)) else math.round(combRns)
                    }*/
                } else if (lfOpt.isEmpty && sfOpt.isDefined) {
                  /*val msg = s"~2($fcdtStr, $htlcd, $segcd, $lvdtStr) when lfOpt.isEmpty && sfOpt.isDefined"
                  logger.info(msg)*/
                  val sfArr = sfOpt.get
                  if (otbOpt.isDefined) {
                    val otbRow = otbOpt.get.toArray
                    val otbColIdx = advBkPeriod2ColIdx(currAdvBkDay, matrixIdx2ColsMap) //+ 1
                    (0 until MAT_COL_LEN).map {
                      cIdx =>
                        if (cIdx < otbColIdx) sfArr(cIdx) else otbRow(cIdx)
                    }
                  } else {
                    val msg = s"($fcdtStr, $htlcd, $segcd, $lvdtStr) otb not found when lfOpt.isEmpty && sfOpt.isDefined"
                    logger.warn(msg)
                    sfArr.toIndexedSeq
                  }
                  /*(0 until MAT_COL_LEN).map {
                    cIdx =>
                      val shortTermRns = sfOpt.get(cIdx)
                      val combRns = shortTermRns/* * usingWeight._2*/
                      //if (combRns < 0) -math.round(math.abs(combRns)) else math.round(combRns)
                      combRns
                  }*/

                }else if (lfOpt.isEmpty && sfOpt.isEmpty) {
                    val otbRow = otbOpt.get.toArray
//                    println(currAdvBkDay)
                    val otbColIdx = advBkPeriod2ColIdx(currAdvBkDay, matrixIdx2ColsMap) //+ 1
                    (0 until MAT_COL_LEN).map {
                      cIdx =>
                        if (cIdx < otbColIdx) 0 else otbRow(cIdx)
                    }
                }
                else {
                  // lfOpt.isDefined && sfOpt.isEmpty
                  if (otbOpt.isDefined) {
                    val lfRow = lfOpt.get
                    val otbRow = otbOpt.get.toArray
                    //val resRow = Array.emptyIntArray
                    val otbColIdx = advBkPeriod2ColIdx(currAdvBkDay, matrixIdx2ColsMap) //+ 1
                    /*val msg = s"~3($fcdtStr, $htlcd, $segcd, $lvdtStr) otb found when lfOpt.isDefined && sfOpt.isEmpty"
                    logger.info(msg)*/
                    (0 until MAT_COL_LEN).map {
                      cIdx =>
                        val lfRns = lfRow(cIdx)
                        val otbRns = otbRow(cIdx)
                        val combRns = if (cIdx >= otbColIdx) /*lfRns + */ otbRns else lfRns * usingWeight._1
                        if (combRns < 0) -math.round(math.abs(combRns)) else math.round(combRns)
                    }
                  } else {
                    val msg = s"($fcdtStr, $htlcd, $segcd, $lvdtStr) otb not found when lfOpt.isDefined && sfOpt.isEmpty"
                    logger.warn(msg)
                    lfOpt.get.toIndexedSeq
                  }
                }

                s"$fcdtStr#$htlcd#$segcd#$lvdtStr#" + combResult.mkString(fieldSplitter)
            }
        }

        val notPredCombineResult = usingNotPredShorttermKv.mapPartitions {
          par =>
            par.map {
              case ((fcdtStr, htlcd, segcd, lvdtStr), sf) =>
                s"$fcdtStr#$htlcd#$segcd#$lvdtStr#" + sf.mkString(fieldSplitter)
            }
        }
//        notPredCombineResult.foreach(println)
        val result = predCombineResult.union(notPredCombineResult)
        if (saveToFile) {
          if (distDir.startsWith("hdfs://")) {
            HDFSUtil.delete(hadoopHost, distPath)
          } else {
            val path: Path = Path(distPath)
            path.deleteRecursively()
          }
          result.saveAsTextFile(distPath)
        }
      }
      println("Combine finished")
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

  private def seasonWeekday2Tag(seasonNum: Int) = {
    (seasonNum + 1) * 10
  }

  private def seasonWeekday2Tag(seasonNum: Int, weekday: Int, weekendDays: Array[Int]) = {
    val dayTag = if (weekendDays.contains(weekday)) 1 else 2
    (seasonNum + 1) * 10 + dayTag
  }

  private def seasonWeekday2Tag(seasonNum: Int, weekday: Int) = {
    (seasonNum + 1) * 10 + weekday
  }

  private def dt2Tag(lvdt: DateTime,
                     seasonRows: Array[Array[String]],
                     specialDaysRows: Array[Array[String]],
                     seasonWeekTagType: String,
                     weekendDays: Array[Int],
                     htlcd: String, spcialDaysTagEnable: Boolean) = {
    val lvdtNum = lvdt.toString("yyyyMMdd").toInt
    val lvdtStr = lvdt.toString("yyyy-MM-dd")
    val weekDay = lvdt.getDayOfWeek
    implicit val ordrMinSeason = Ordering.by {
      (r: Array[String]) =>
        val start = r(seasonColIdx("start_dt")).replaceAll("-", "").toInt
        val end = r(seasonColIdx("end_dt")).replaceAll("-", "").toInt
        end - start
    }

    val htlSeason = seasonRows.filter {
      r =>
        val htl = r(seasonColIdx("htl_cd"))
        htl == htlcd
    }

    val season = htlSeason.filter {
      r =>
        val startDt = r(seasonColIdx("start_dt"))
        val endDt = r(seasonColIdx("end_dt"))
        val startNum = startDt.replaceAll("-", "").toInt
        val endNum = endDt.replaceAll("-", "").toInt
        //println(s"lvdtNum = $lvdtNum, startNum = $startNum, endNum = $endNum")
        lvdtNum >= startNum && lvdtNum <= endNum
    }

    if (season.isEmpty) {
      val msg = s"Cannot find season of $htlcd and $lvdtStr"
      println(msg)
      logger.error(msg)
    }

    val minSeason = season.min(ordrMinSeason)
    val special = if (spcialDaysTagEnable)
      specialDaysRows.filter {
        r =>
          lvdtStr == r(holidaysColIdx("date"))
      } else Array.empty[Array[String]]

    val seasonNum = minSeason(seasonColIdx("season")).toInt
    val seasonWeekTag = seasonWeekTagType match {
      case "weekendOrNot" => seasonWeekday2Tag(seasonNum, weekDay, weekendDays)
      case "noWeekday" => seasonWeekday2Tag(seasonNum)
      case _ => seasonWeekday2Tag(seasonNum, weekDay)
    }

    val specialTag = if (special.isEmpty) None else Some(special.map(r => r(holidaysColIdx("tag")).toInt))
    (htlcd, (seasonWeekTag, specialTag))
  }

  def genOrigMat(rawData: RDD[Array[String]],
                 startDt: String, endDt: String,
                 predSegConfDistinct: Array[(String, String)],
                 matrixIdx2ColsMap: IndexedSeq[(Int, (Int, Int))],
                 faultTolerance: Boolean) = {
    val startDtNum = startDt.toInt
    val endDtNum = endDt.toInt
    val preMatRows = rawData.filter {
      r =>
        val dtNum = r(segBkDailySumColIdx("live_dt")).replaceAll("-", "").toInt
        val htlcd = r(segBkDailySumColIdx("htl_cd"))
        val segcd = r(segBkDailySumColIdx("seg_cd"))

        val advBkDays = r(segBkDailySumColIdx("adv_bk_days")).toInt
        val advBkDaysCheck = if (faultTolerance) {
          advBkDays >= 0
        } else true

        advBkDaysCheck && dtNum > startDtNum && dtNum <= endDtNum && predSegConfDistinct.contains((htlcd, segcd))
    }.mapPartitions {
      par =>
        par.map {
          r =>
            val htlcd = r(segBkDailySumColIdx("htl_cd"))
            val segcd = r(segBkDailySumColIdx("seg_cd"))
            val liveDt = r(segBkDailySumColIdx("live_dt")) //.replaceAll("-", "").toInt
          val advBkDays = r(segBkDailySumColIdx("adv_bk_days")).toInt
            val rnsTmp = r(segBkDailySumColIdx("rns")).toFloat
            val rns = if (rnsTmp < 0) -math.round(math.abs(rnsTmp)) else math.round(rnsTmp)
            val colIdx = advBkPeriod2ColIdx(advBkDays, matrixIdx2ColsMap)
            ((htlcd, segcd, liveDt, colIdx), rns)
        }
    }.reduceByKey(_ + _)

    val entireCols = matrixIdx2ColsMap.indices.map(i => i)
    val result = preMatRows.mapPartitions {
      par =>
        par.map {
          case ((htlcd, segcd, liveDt, colIdx), rns) =>
            ((htlcd, segcd, liveDt), (colIdx, rns))
        }
    }.groupByKey.mapPartitions {
      par =>
        par.map {
          case ((htlcd, segcd, liveDt), vLst) =>
            val colIdxList = vLst.toList.map(_._1).union(entireCols).distinct
            val vMap = vLst.toMap
            val base = colIdxList.map {
              cIdx =>
                (cIdx, if (vMap.get(cIdx).isDefined) vMap(cIdx) else 0)
            }
            val res = base.sortBy(_._1)
            ((htlcd, segcd, liveDt), res.map(_._2))
        }
    }
    result
  }

  def advBkPeriod2ColIdx(origAdvBkPeriod: Int, matrixIdx2ColsMap: IndexedSeq[(Int, (Int, Int))]) = {
    val maxCol = matrixIdx2ColsMap.last._2._2
    val advBkPeriod = if (origAdvBkPeriod >= maxCol) maxCol else origAdvBkPeriod
    val res = matrixIdx2ColsMap.filter {
      case (colIdx, (startPeriod, endPeriod)) =>
        advBkPeriod >= startPeriod && advBkPeriod <= endPeriod
    }

    if (res.isEmpty) {
      val msg = s"Cannot find colIdx from matrixIdx2ColsMap, origAdvBkPeriod = $origAdvBkPeriod, advBkPeriod = $advBkPeriod"
      logger.error(msg)
      println(msg)
    }
    res.head._1
  }

  private def matResultColIdx(colName: String) = colName match {
    case "fc_dt" => 0
    case "htl_cd" => 1
    case "seg_cd" => 2
    case "live_dt" => 3
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

  private def segPredConfColIdx(colName: String) = colName match {
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

  private def holidaysColIdx(colName: String) = colName match {
    case "desp" => 0
    case "date" => 1
    case "tag" => 2
    case "year" => 3
    case "holiday_key" => 4
    case "offset" => 5
  }

  private def seasonColIdx(colName: String) = colName match {
    case "htl_cd" => 0
    case "season" => 1
    case "base_rt_cd" => 2
    case "base_rt_price" => 3
    case "month_no" => 4
    case "start_dt" => 5
    case "end_dt" => 6
    case "update_dt" => 7
  }

  private def segBkDailySumColIdx(colName: String) = colName match {
    case "htl_cd" => 0
    case "seg_cd" => 1
    case "is_member" => 2
    case "live_dt" => 3
    case "adv_bk_days" => 4
    case "rns" => 5
    case "rev" => 6
  }

}
