package cn.jw.rms.ab.pred.longterm

import java.io.Serializable

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
import scala.collection.Map


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
      println("Long term started.")
      val checkpointDir = config.getString("checkpoint-dir")
      sc.setCheckpointDir(checkpointDir)
      val checkpointNum = config.getInt("checkpoint-num")
      val segbkdailysumDir = config.getString("segbkdailysum-dir")
      val segpredconfDir = config.getString("predconf-dir")
      val seasonDir = config.getString("season-dir")
      val holidayDir = config.getString("specialdays-dir")
      val fieldSplitter = config.getString("field-splitter")
      val distDir = config.getString("dist-dir")
      val pairRDDReparNum = config.getInt("pairRDD-repartition-num")
      val RDDReparNum = config.getInt("RDD-repartition-num")
      val fcDaysCount = config.getInt("fc-days")
      val defaultHistDays = config.getInt("default-hist-days")
      val commonHtlcd = config.getString("common-htlcd")
      val hotelDir = config.getString("hotel-dir")
      val fcHtl = config.getString("fc-htlcd")
      val saveToFile = config.getBoolean("save-result-to-file")
      val usingPreStepRDD = config.getBoolean("using-prevStepRDD")
      val hadoopHost = config.getString("hadoop-host")
      val testOpen = config.getBoolean("hist-enable")
      val testSegbkdailysumDir = config.getString("hist-segbkdailysum-dir")
      val HAStartStr = config.getString("hist-start")
      val HAEndStr = config.getString("hist-end")
      val seasonWeekTagType = config.getString("season-weekday-tag-type")
      val weekendDays = config.getIntList("weekend-days").map(_.toInt).toArray
      val matrixCols = config.getIntList("matrix-columns").map(_.toInt).toArray
      val faultTolerance = config.getBoolean("fault-tolerance")
      val segType = config.getString("seg-type")
      val fcDateStr = config.getString("fc-date")
      val invPercentDir = config.getString("inv-percent-dir")
      val invPercentMap = sc.textFile(invPercentDir).map(_.split(fieldSplitter)).map {
        line =>
          val htlCd = line(invPercentColIdx("htl_cd"))
          val totalInv = line(invPercentColIdx("total_inv"))
          val currentInv = line(invPercentColIdx("current_inv"))
          val startdt = line(invPercentColIdx("startdt"))
          val enddt = line(invPercentColIdx("enddt"))
          (htlCd, (totalInv, currentInv, startdt, enddt))
      }.groupByKey().collectAsMap()
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

      val seasonRaw = sc.textFile(seasonDir).filter(_.nonEmpty).map(_.split(fieldSplitter, -1)).filter {
        r =>
          if (fcHtl == "all")
            r(seasonColIdx("htl_cd")) != commonHtlcd
          else {
            val htlList = fcHtl.split(",")
            htlList.contains(r(seasonColIdx("htl_cd")))
          }
      }.collect()
      val holidayRaw = sc.textFile(holidayDir).filter(_.nonEmpty).map(_.split(fieldSplitter, -1)).collect()

      val hotelRaw = sc.textFile(hotelDir).filter(_.nonEmpty).map(_.split(fieldSplitter, -1)).filter {
        r =>
          if (fcHtl == "all")
            r(hotelColIdx("htl_cd")) != commonHtlcd
          else {
            val htlList = fcHtl.split(",")
            htlList.contains(r(hotelColIdx("htl_cd")))
          }
      }.map {
        r =>
          (r(hotelColIdx("htl_cd")), 1)
      }

      val parHotelRaw = if (pairRDDReparNum > 0) hotelRaw.partitionBy(new HashPartitioner(pairRDDReparNum)) else hotelRaw

      val hotelCds = parHotelRaw.collect().map(_._1)

      val segConfRaw = sc.textFile(segpredconfDir).filter(_.nonEmpty).map(_.split(fieldSplitter, -1))
      val segConfRawMap = segConfRaw.filter {
        r =>
          r(segPredConfColIdx("is_predict")) == "Y"
      }.map {
        r =>
          (r(segPredConfColIdx("htl_cd")), r(segPredConfColIdx("seg_cd")))
      }
      val parSegConfRawMap = if (pairRDDReparNum > 0) segConfRawMap.partitionBy(new HashPartitioner(pairRDDReparNum)) else segConfRawMap

      val joinParSegConfRawMap = parHotelRaw.join(parSegConfRawMap).map {
        case (htlcd, (_, segcd)) =>
          (htlcd, segcd)
      }.distinct()

      val predSegConfDistinct =
        if (segType == "FIT_TOTAL") {
          joinParSegConfRawMap.map(_._1).distinct.map {
            htl =>
              (htl, "FIT_TOTAL")
          }
        } else joinParSegConfRawMap

      val startDt = fcDatePair._1
      val endDt = fcDatePair._2
      val fcDays = Days.daysBetween(startDt, endDt).getDays

      val fcDateTags = hotelCds.flatMap {
        htl => (0 until fcDaysCount + fcDays).map {
          day =>
            val dt = startDt.plusDays(day + 1)
            val tag = dt2Tag(dt, seasonRaw, holidayRaw, seasonWeekTagType, weekendDays, htl)
            ((tag._1, dt.toString("yyyyMMdd").toInt), tag._2)
        }
      }.toMap

      val specDaysFcDate = fcDateTags.filter(_._2._2.isDefined).toArray.map{
        case ((htl, dt), _) =>
          (htl, dt)
      }

      /*val specDaysFcDateTmpTags = fcDateTags.filter(_._2._2.isDefined).toArray
      println("specDaysFcDateTmpTags")
      specDaysFcDateTmpTags.foreach{
        case ((htl, dt), (seaWeekTag, specTagOpt)) =>
          println(s"$htl#$dt#$seaWeekTag#${specTagOpt.get.mkString("#")}")
      }*/

      val seasonWeekDayFcTags = fcDateTags.map{
        case ((htl, dt), (tag, _)) =>
          ((htl, dt), tag)
      }


      val HAStartDt = if (testOpen) new DateTime(HAStartStr) else startDt.minusDays(defaultHistDays)
      val HAEndDt = if (testOpen) new DateTime(HAEndStr) else endDt
      val histDays = if (testOpen) Days.daysBetween(HAStartDt, HAEndDt).getDays else defaultHistDays

      val histDateTags = hotelCds.flatMap {
        htl =>
          (0 to histDays).map {
            day =>
              val dt = HAStartDt.plusDays(day)
              val tag = dt2Tag(dt, seasonRaw, holidayRaw, seasonWeekTagType, weekendDays, htl)
              ((tag._1, dt.toString("yyyyMMdd").toInt), tag._2)
          }.union {
            (1 to fcDays).map {
              day =>
                val dt = startDt.plusDays(day)
                val tag = dt2Tag(dt, seasonRaw, holidayRaw, seasonWeekTagType, weekendDays, htl)
                ((tag._1, dt.toString("yyyyMMdd").toInt), tag._2)
            }
          }
      }.toMap

      /*println("histDateTags:")
      histDateTags.foreach(println)*/

      val testHAMat = if (testOpen) {
        val origMatStartDtStr = HAStartDt.toString("yyyyMMdd")
        val origMatEndDtStr = HAEndDt.toString("yyyyMMdd")
        val testDailySumRaw = if (RDDReparNum > 0)
          sc.textFile(testSegbkdailysumDir).filter(_.nonEmpty).map(_.split(fieldSplitter, -1)).repartition(RDDReparNum)
        else
          sc.textFile(testSegbkdailysumDir).filter(_.nonEmpty).map(_.split(fieldSplitter, -1))

        val origMat = genOrigMat(testDailySumRaw, origMatStartDtStr, origMatEndDtStr, predSegConfDistinct, matrixIdx2ColsMap, faultTolerance, pairRDDReparNum)
        val ha = genHA(origMat, histDateTags, MAT_COL_LEN).cache()
        Some(ha)
      } else None

      /*println("testHAMat")
       testHAMat.get.collect().foreach{
         case ((htlcd, segcd, tag), avgList) =>
           val str = s"$htlcd#$segcd#$tag#${avgList.mkString("#")}"
           println(str)
       }*/


      val result = (0 to fcDays).map {
        day =>
          val fcDate = startDt.plusDays(day)
          val fcDateStr = fcDate.toString("yyyy-MM-dd")
          val fcDateSimpStr = fcDate.toString("yyyyMMdd")
          logger.info(s"fcDate = $fcDateStr")

          val fcLvdtTags = hotelCds.flatMap {
            htl =>
              (0 until fcDaysCount).flatMap {
                idx =>
                  val lvdt = fcDate.plusDays(idx + 1)
                  val lvdtStr = lvdt.toString("yyyy-MM-dd")
                  val lvdtNum = lvdt.toString("yyyyMMdd").toInt
                  val lvdtTag = fcDateTags((htl, lvdtNum))
                  val seasonWeekDayTag = lvdtTag._1
                  val specialDayTag = lvdtTag._2
                  if (specialDayTag.isDefined) {
                    specialDayTag.get.map(tag => (htl, tag, lvdtNum))
                  } else Array((htl, seasonWeekDayTag, lvdtNum))
              }
          }

          val haMat = if (testHAMat.isEmpty) {
            val segbkdailysumPath = segbkdailysumDir + "/dt=" + fcDateSimpStr
            val dailyRaw = if (RDDReparNum > 0)
              sc.textFile(segbkdailysumPath).filter(_.nonEmpty).map(_.split(fieldSplitter, -1)).repartition(RDDReparNum)
            else
              sc.textFile(segbkdailysumPath).filter(_.nonEmpty).map(_.split(fieldSplitter, -1))
            val origMatStartDtStr = fcDate.minusDays(defaultHistDays).toString("yyyyMMdd")
            val origMatEndDtStr = fcDateSimpStr
            val origMat = genOrigMat(dailyRaw, origMatStartDtStr, origMatEndDtStr, predSegConfDistinct, matrixIdx2ColsMap, faultTolerance, pairRDDReparNum)
            genHA(origMat, histDateTags, MAT_COL_LEN)
          } else testHAMat.get

          val predStep1 = haMat.mapPartitions {
            par =>
              par.flatMap {
                case ((htlcd, segcd, currTag), values) =>
                  fcLvdtTags.filter {
                    r =>
                      val htl = r._1
                      val tag = r._2
                      htlcd == htl && currTag == tag
                  }.map {
                    case (_, _, lvdt) =>
                      ((htlcd, segcd, lvdt), values)
                  }
              }
          }.cache()

          //
          val predSpecDays = predStep1.filter{
            case ((htlcd, _, lvdt), _) =>
              specDaysFcDate.contains((htlcd, lvdt))
          }.map{
            case ((htlcd, _, lvdt), _) =>
              (htlcd, lvdt)
          }.collect()

          val currStartLvDtNum = fcDate.plusDays(1).toString("yyyyMMdd").toInt
          val currEndLvDtNum = fcDate.plusDays(fcDaysCount).toString("yyyyMMdd").toInt

          val nonePredSpecDays = specDaysFcDate.filter{
            case (htlcd, lvdt) =>
              lvdt >= currStartLvDtNum &&
                lvdt <= currEndLvDtNum &&
              !predSpecDays.contains((htlcd, lvdt))
          }

          val predStep2 =
            if(nonePredSpecDays.isEmpty) predStep1
            else {
              val nonePredSeaWeekTag = seasonWeekDayFcTags.filter{
                case ((htl, dt), tag) =>
                  nonePredSpecDays.contains((htl, dt))
              }.toArray

              println("nonePredSeaWeekTag")
              nonePredSeaWeekTag.foreach(println)

              val redoRows = haMat.filter{
                case ((htlcd, segcd, tag), avgList) =>
                  nonePredSeaWeekTag.exists{
                    case ((htl, lvdt), seaTag) =>
                      htl == htlcd && tag == seaTag
                  }
              }.flatMap{
                case ((htlcd, segcd, tag), avgList) =>
                  nonePredSeaWeekTag.filter{
                    case ((htl, lvdt), seaTag) =>
                      htl == htlcd && tag == seaTag
                  }.map{
                    case ((_, lvdt), _) =>
                      ((htlcd, segcd, lvdt), avgList)
                  }
              }

              predStep1.union(redoRows)
            }

          val pred = predStep2.aggregateByKey((0, (0 until MAT_COL_LEN).map(i => 0).toList))(
            {
              case ((cnt: Int, sum: List[Int]), (v1: List[Int])) =>
                (cnt + 1, (sum, v1).zipped.map(_ + _))
            }, {
              case ((count, sum),
              (otherCount, otherSum)) =>
                (count + otherCount, (sum, otherSum).zipped.map(_ + _))
            }
          ).mapPartitions {
            par =>
              par.map {
                case ((htlcd, segcd, lvdt), (cnt, sumList)) =>
                  val (totalinv, currentinv) = if (invPercentMap.contains(htlcd)) {
                    val dtSet: Array[(String, String, String, String)] = invPercentMap(htlcd).toArray
                    val betweenDay = dtSet.find {
                      case (a, b, stStr, etStr) =>
                        val st = stStr.replace("-","").toInt
                        val et = etStr.replace("-","").toInt
                        st <= lvdt && et >= lvdt
                    }
                    val resultInv = if (betweenDay.isDefined)
                      betweenDay.get
                    else
                      ("1", "1", "1", "1")
                    (resultInv._1, resultInv._2)
                  } else ("1", "1")
                  val invpercent = currentinv.toFloat / totalinv.toFloat
                  //   println(s"lvdt is: $lvdt  , Invpercent is :$invpercent")
                  val avgList = if (cnt > 1) {
                    sumList.map {
                      s =>
                        val avg = s.toFloat / cnt * invpercent
                        if (avg < 0) -math.round(math.abs(avg)) else math.round(avg)
                    }
                  } else sumList.map {
                    s =>
                      val sl = s * invpercent
                      math.round(sl)
                  }
                  val lvdtStr = DateTime.parse(lvdt.toString, DateTimeFormat.forPattern("yyyyMMdd")).toString("yyyy-MM-dd")
                  s"$fcDateStr#$htlcd#$segcd#$lvdtStr#${avgList.mkString("#")}"
              }
          }

          /*val pred = haMat.mapPartitions {
            par =>
              par.flatMap {
                case ((htlcd, segcd, currTag), values) =>
                  fcLvdtTags.filter {
                    r =>
                      val htl = r._1
                      val tag = r._2
                      htlcd == htl && currTag == tag
                  }.map {
                    case (_, _, lvdt) =>
                      ((htlcd, segcd, lvdt), values)
                  }
              }
          }.aggregateByKey((0, (0 until MAT_COL_LEN).map(i => 0).toList))(
            {
              case ((cnt: Int, sum: List[Int]), (v1: List[Int])) =>
                (cnt + 1, (sum, v1).zipped.map(_ + _))
            }, {
              case ((count, sum),
              (otherCount, otherSum)) =>
                (count + otherCount, (sum, otherSum).zipped.map(_ + _))
            }
          ).mapPartitions {
            par =>
              par.map {
                case ((htlcd, segcd, lvdt), (cnt, sumList)) =>
                  val (totalinv, currentinv) = if (invPercentMap.contains(htlcd)) {
                    val dtSet: Array[(String, String, String, String)] = invPercentMap(htlcd).toArray
                    val betweenDay = dtSet.find {
                      case (a, b, st, et) =>
                        st <= lvdt && et >= lvdt
                    }
                    val resultInv = if (betweenDay.isDefined)
                      betweenDay.get
                    else
                      ("1", "1", "1", "1")
                    (resultInv._1, resultInv._2)
                  } else ("1", "1")
                  val invpercent = currentinv.toFloat / totalinv.toFloat
                  //   println(s"lvdt is: $lvdt  , Invpercent is :$invpercent")
                  val avgList = if (cnt > 1) {
                    sumList.map {
                      s =>
                        val avg = s.toFloat / cnt * invpercent
                        if (avg < 0) -math.round(math.abs(avg)) else math.round(avg)
                    }
                  } else sumList.map {
                    s =>
                      val sl = s * invpercent
                      math.round(sl)
                  }
                  s"$fcDateStr#$htlcd#$segcd#$lvdt#${avgList.mkString("#")}"
              }
          }*/

          if (!testOpen && saveToFile) {
            val distPath = distDir + "/dt=" + fcDateSimpStr
            if (distDir.startsWith("hdfs://")) {
              HDFSUtil.delete(hadoopHost, distPath)
            } else {
              val path: Path = Path(distPath)
              path.deleteRecursively()
            }
            pred.saveAsTextFile(distPath)
          }
          pred
      }

      if (testOpen && saveToFile) {
        val distPath = distDir
        if (distDir.startsWith("hdfs://")) {
          HDFSUtil.delete(hadoopHost, distPath)
        } else {
          val path: Path = Path(distPath)
          path.deleteRecursively()
        }
        //result.reduce((rdd1, rdd2) => rdd1.union(rdd2)).saveAsTextFile(distPath)
        if (result.nonEmpty) {
          var outputRDD = result(0)
          (1 until result.length).foreach {
            idx =>
              val currRDD = result(idx)
              outputRDD = outputRDD /*.repartition(reparNum)*/ .cache().union(currRDD)
              if (idx % checkpointNum == 0) {
                outputRDD.checkpoint()
                outputRDD.count()
              }
          }
          (if (RDDReparNum > 0)
            outputRDD.repartition(RDDReparNum)
          else outputRDD).saveAsTextFile(distPath)
        }
      }
      println("Long term finished")
      None
    } match {
      case Success(res) =>
        gotSucceed = true
        None
      case Failure(e) =>
        gotSucceed = false
        errMsg = e.toString
        logger.error("error: " + e.toString)
        e.printStackTrace()
        None
    }
  }

  def genOrigMat(rawData: RDD[Array[String]],
                 startDt: String, endDt: String,
                 predSegConfDistinct: RDD[(String, String)],
                 matrixIdx2ColsMap: IndexedSeq[(Int, (Int, Int))],
                 faultTolerance: Boolean,
                 pairRDDReparNum: Int) = {
    val startDtNum = startDt.toInt
    val endDtNum = endDt.toInt
    val preMatRows = rawData.filter {
      r =>
        val dtNum = r(segBkDailySumColIdx("live_dt")).replaceAll("-", "").toInt
        val advBkDays = r(segBkDailySumColIdx("adv_bk_days")).toInt
        val advBkDaysCheck = if (faultTolerance) {
          advBkDays >= 0
        } else true

        advBkDaysCheck && dtNum >= startDtNum && dtNum <= endDtNum
    }.mapPartitions {
      par =>
        par.map {
          r =>
            val htlcd = r(segBkDailySumColIdx("htl_cd"))
            val segcd = r(segBkDailySumColIdx("seg_cd"))
            val liveDt = r(segBkDailySumColIdx("live_dt")).replaceAll("-", "").toInt
            val advBkDays = r(segBkDailySumColIdx("adv_bk_days")).toInt
            val rns = r(segBkDailySumColIdx("rns")).toDouble
            val colIdx = advBkPeriod2ColIdx(advBkDays, matrixIdx2ColsMap)
            (htlcd, (segcd, liveDt, colIdx, rns))
        }
    }

    val parPreMatRows = if (pairRDDReparNum > 0) preMatRows.partitionBy(new HashPartitioner(pairRDDReparNum)) else preMatRows

    val joinParPreMatRows = predSegConfDistinct.join(parPreMatRows).filter {
      case (_, (predSegcd, (segcd, _, _, _))) =>
        predSegcd == segcd
    }

    val preMatSumRows = joinParPreMatRows.mapPartitions {
      par =>
        par.map {
          case (htlcd, (_, (segcd, liveDt, colIdx, rns))) =>
            ((htlcd, segcd, liveDt, colIdx), rns)
        }
    }.reduceByKey(_ + _)

    val entireCols = matrixIdx2ColsMap.indices.map(i => i)
    val result = preMatSumRows.mapPartitions {
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
                (cIdx, if (vMap.get(cIdx).isDefined) vMap(cIdx) else 0.0)
            }
            val res = base.sortBy(_._1)
            ((htlcd, segcd, liveDt), res.map(_._2.toDouble))
        }
    }
    result
  }

  private def genHA(origMat: RDD[((String, String, Int), List[Double])],
                    dateTags: Map[(String, Int), (Int, Option[Array[Int]])],
                    MAT_COL_LEN: Int) = {

    val allTagRows = origMat.mapPartitions {
      par =>
        par.map {
          case ((htlcd, segcd, liveDtNum), r) =>
            val tag = dateTags((htlcd, liveDtNum))
            val seasonWeekDayTag = tag._1
            val specialDayTag = tag._2
            val rList = if (specialDayTag.isDefined) {
              if (specialDayTag.get.length > 1)
                specialDayTag.get.map(tag => ((htlcd, segcd, tag), r))
              else Array(((htlcd, segcd, specialDayTag.get.head), r))
            } else Array(((htlcd, segcd, seasonWeekDayTag), r))
            rList
        }.flatten
    }

    val ha = allTagRows.aggregateByKey((0, (0 until MAT_COL_LEN).map(i => 0.0).toList))(
      {
        case ((cnt: Int, sum: List[Double]), (v1: List[Double])) =>
          (cnt + 1, (sum, v1).zipped.map(_ + _))
      }, {
        case ((count, sum),
        (otherCount, otherSum)) =>
          (count + otherCount, (sum, otherSum).zipped.map(_ + _))
      }
    ).mapPartitions {
      par =>
        par.map {
          case ((htlcd, segcd, tag), (cnt, sumList)) =>
            val avgList = sumList.map {
              s =>
                val avg = s / cnt
                if (avg < 0) -math.round(math.abs(avg)).toInt else math.round(avg).toInt
            }
            ((htlcd, segcd, tag), avgList)
        }
    }
    ha
  }

  private def dt2Tag(lvdt: DateTime,
                     seasonRows: Array[Array[String]],
                     specialDaysRows: Array[Array[String]],
                     seasonWeekTagType: String,
                     weekendDays: Array[Int],
                     htlcd: String) = {
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
        htl == htlcd
    }.filter {
      r =>
        val startDt = r(seasonColIdx("start_dt"))
        val endDt = r(seasonColIdx("end_dt"))
        val startNum = startDt.replaceAll("-", "").toInt
        val endNum = endDt.replaceAll("-", "").toInt
        lvdtNum >= startNum && lvdtNum <= endNum
    }.min(ordrMinSeason)

    val special = specialDaysRows.filter {
      r =>
        lvdtStr == r(holidaysColIdx("date"))
    }

    val seasonNum = season(seasonColIdx("season")).toInt
    val seasonWeekTag = seasonWeekTagType match {
      case "weekendOrNot" => seasonWeekday2Tag(seasonNum, weekDay, weekendDays)
      case "noWeekday" => seasonWeekday2Tag(seasonNum)
      case _ => seasonWeekday2Tag(seasonNum, weekDay)
    }

    val specialTag = if (special.isEmpty) None else Some(special.map(r => r(holidaysColIdx("tag")).toInt))
    (htlcd, (seasonWeekTag, specialTag))
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

  private def segBkDailySumColIdx(colName: String) = colName match {
    case "htl_cd" => 0
    case "seg_cd" => 1
    case "is_member" => 2
    case "live_dt" => 3
    case "adv_bk_days" => 4
    case "rns" => 5
    case "rev" => 6
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

  private def invPercentColIdx(colName: String) = colName match {
    case "htl_cd" => 0
    case "total_inv" => 1
    case "current_inv" => 2
    case "startdt" => 3
    case "enddt" => 4
  }
}
