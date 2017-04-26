package cn.jw.rms.ab.pred.shortterm

import cn.jw.rms.data.framework.common.rules.PredictModelAssembly
import cn.jw.rms.data.framework.common.utils.HDFSUtil
import com.typesafe.config.Config
import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.rdd.RDD
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, Days}

import scala.reflect.io.Path
import scala.util.{Failure, Success, Try}
import breeze.linalg._
import cn.deanzhang.spark.knn.KNN

import collection.JavaConversions._
import cn.jw.rms.ab.pred.shortterm.common.FieldsDef._
import cn.jw.rms.ab.pred.shortterm.common.SeriLogger._
import cn.jw.rms.ab.pred.shortterm.common.Kernel._


class Predictor extends PredictModelAssembly with Serializable {

  var gotSucceed = false
  var errMsg = ""

  override def accuracy(sc: SparkContext, config: Config, prevStepRDD: Option[RDD[String]]): String = "Not Supported"

  override def succeed: (Boolean, String) = (gotSucceed, errMsg)

  override def predict(sc: SparkContext, config: Config, prevStepRDD: Option[RDD[String]]): Option[RDD[String]] = {
    Try {
      println("Short term started")
      val checkpointDir = config.getString("checkpoint-dir")
      sc.setCheckpointDir(checkpointDir)

      val segbkdailysumDir = config.getString("segbkdailysum-dir")
      val segpredconfDir = config.getString("predconf-dir")
      val seasonDir = config.getString("season-dir")
      val holidayDir = config.getString("specialdays-dir")
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
      val checkpointNum = config.getInt("checkpoint-num")
      val histStartStr = config.getString("hist-start")
      val histEndStr = config.getString("hist-end")
      val defaultHistDays = config.getInt("default-hist-days")
      val m = config.getInt("m")
      val singularW = config.getString("singular-w")
      val singularWArr = singularW.split(fieldSplitter).map(_.toDouble)
      val seasonWeekTagType = config.getString("season-weekday-tag-type")
      val weekendDays = config.getIntList("weekend-days").map(_.toInt).toArray
      val matrixCols = config.getIntList("matrix-columns").map(_.toInt).toArray
      val faultTolerance = config.getBoolean("fault-tolerance")
      val printW = config.getBoolean("print-w")
      val knnK = config.getInt("knn.k")
      val segType = config.getString("seg-type")
      val fcDateConfigStr = config.getString("fc-date")
      val fcDatePair =
        if (fcDateConfigStr == "now") {
          val dt = new DateTime(DateTime.now().toString("yyyy-MM-dd")).minusDays(1)
          (dt, dt)
        } else {
          val dts = fcDateConfigStr.split(",")
          val startDt = new DateTime(dts(0)).minusDays(1)
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

      val segConfRaw = sc.textFile(segpredconfDir).filter(_.nonEmpty).map(_.split(fieldSplitter, -1))
      val segConfRawMap = segConfRaw.filter {
        r =>
          r(segPredConfColIdx("is_predict")) == "Y"
      }.map {
        r =>
          (r(segPredConfColIdx("htl_cd")), r(segPredConfColIdx("seg_cd")))
      }
      val parSegConfRawMap = if (pairRDDReparNum > 0) segConfRawMap.partitionBy(new HashPartitioner(pairRDDReparNum)) else segConfRawMap

      val joinParSegConfRawMap = parHotelRaw.join(parSegConfRawMap).map{
        case (htlcd, (_, segcd)) =>
          (htlcd, segcd)
      }.distinct()

      val segConfDistinct =
        if(segType == "FIT_TOTAL"){
          joinParSegConfRawMap.map (_._1).distinct.map{
            htl =>
              (htl, "FIT_TOTAL")
        }
      } else joinParSegConfRawMap

      val startDt = fcDatePair._1
      val endDt = fcDatePair._2
      val fcDays = Days.daysBetween(startDt, endDt).getDays

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

      val singularVector = DenseVector(singularWArr: _*)

      val testEntireMat = if (testOpen) {
        val entireStartDt = new DateTime(histStartStr)
        val entireEndDt = endDt.plusDays(fcDaysCount)
        val entireStartDtStr = entireStartDt.toString("yyyyMMdd")
        val entireEndDtStr = entireEndDt.toString("yyyyMMdd")
        logger.info(s"entireStartDtStr = $entireStartDtStr, entireEndDtStr = $entireEndDtStr")

        val testDailySumRaw = if (RDDReparNum > 0)
          sc.textFile(testSegbkdailysumDir).filter(_.nonEmpty).map(_.split(fieldSplitter, -1)).repartition(RDDReparNum)
        else
          sc.textFile(testSegbkdailysumDir).filter(_.nonEmpty).map(_.split(fieldSplitter, -1))

        val mat = genOrigMat(testDailySumRaw, entireStartDtStr, entireEndDtStr, segConfDistinct, matrixIdx2ColsMap, faultTolerance, pairRDDReparNum)
        mat.cache()
        Some(mat)
      } else None

      val testHistMat = if (testOpen) {
        val histStartDt = new DateTime(histStartStr)
        val histEndDt = new DateTime(histEndStr)
        val histStartDtNum = histStartDt.toString("yyyyMMdd").toInt
        val histEndDtNum = histEndDt.toString("yyyyMMdd").toInt
        val mat = testEntireMat.get.filter {
          case ((_, _, liveDtNum), _) =>
            liveDtNum >= histStartDtNum && liveDtNum <= histEndDtNum
        }
        mat.cache()
        Some(mat)
      } else None

      val testHistTagRowsMap = if (testOpen) {
        val map = genOrigTagMat(testHistMat.get, seasonRaw, holidayRaw, seasonWeekTagType, weekendDays).mapPartitions {
          par =>
            par.map {
              case ((htlcd, segcd, liveDtNum, usingTag), row) =>
                ((htlcd, segcd, usingTag), row)
            }
        }

        val parMat = if (pairRDDReparNum > 0) map.partitionBy(new HashPartitioner(pairRDDReparNum)) else map
        val groupByParMat = parMat.groupByKey().cache()
        Some(groupByParMat)
      } else None

      val testFutureMat = if (testOpen) {
        val futureStartDtNum = startDt.plusDays(1).toString("yyyyMMdd").toInt
        val futureEndDtNum = endDt.plusDays(fcDaysCount).toString("yyyyMMdd").toInt

        val mat = testEntireMat.get.filter {
          case ((_, _, liveDtNum), _) =>
            liveDtNum >= futureStartDtNum && liveDtNum <= futureEndDtNum
        }
        //val parMat = if (pairRDDReparNum > 0) mat.partitionBy(new HashPartitioner(pairRDDReparNum)) else mat
        mat.cache()
        Some(mat)
      } else None

      /*println("testFutureMat")
      testFutureMat.get.collect().foreach{
        case ((htlcd, segcd, liveDt), row) =>
          println(s"$htlcd#$segcd#$liveDt#${row.mkString("#")}")
      }*/

      val testFutureTagMat = if (testOpen) {
        val mat = genOrigTagMat(testFutureMat.get, seasonRaw, holidayRaw, seasonWeekTagType, weekendDays)
        mat.cache()
        Some(mat)
      } else None

      /*println("testFutureTagMat")
      testFutureTagMat.get.collect().foreach{
        case ((htlcd, segcd, liveDtNum, usingTag), row) =>
          println(s"$htlcd#$segcd#$liveDtNum#$usingTag#${row.mkString("#")}")
      }*/

      val results = (0 to fcDays).map {
        day =>
          val fcDate = startDt.plusDays(day)
          val fcDateStr = fcDate.toString("yyyy-MM-dd")
          val fcDateSimpStr = fcDate.toString("yyyyMMdd")
          val futureStartDt = fcDate.plusDays(1)
          val futureStartDtNum = futureStartDt.toString("yyyyMMdd").toInt

          val dailyEntireMat = if(testOpen) None else {
            val entireStartDt = fcDate.minusDays(defaultHistDays)
            val entireEndDt = fcDate.plusDays(fcDaysCount)
            val entireStartDtStr = entireStartDt.toString("yyyyMMdd")
            val entireEndDtStr = entireEndDt.toString("yyyyMMdd")
            logger.info(s"entireStartDtStr = $entireStartDtStr, entireEndDtStr = $entireEndDtStr")
            val path = segbkdailysumDir + "/dt=" + fcDateSimpStr
            val dailySumRaw = if (RDDReparNum > 0)
              sc.textFile(path).filter(_.nonEmpty).map(_.split(fieldSplitter, -1)).repartition(RDDReparNum)
            else
              sc.textFile(path).filter(_.nonEmpty).map(_.split(fieldSplitter, -1))
            val mat = genOrigMat(dailySumRaw, entireStartDtStr, entireEndDtStr, segConfDistinct, matrixIdx2ColsMap, faultTolerance, pairRDDReparNum)
            Some(mat)
          }

          val futureTagMatWithMinColIdx = {
            val futureEndDt = futureStartDt.plusDays(fcDaysCount)
            val futureEndDtNum = futureEndDt.toString("yyyyMMdd").toInt
            val usingMat = if (testOpen) testFutureTagMat.get
            else {
              val dailyFutureMat = dailyEntireMat.get.filter {
                case ((_, _, liveDtNum), _) =>
                  liveDtNum >= futureStartDtNum && liveDtNum <= futureEndDtNum
              }
              //val parMat = if (pairRDDReparNum > 0) dailyFutureMat.partitionBy(new HashPartitioner(pairRDDReparNum)) else dailyFutureMat
              genOrigTagMat(dailyFutureMat, seasonRaw, holidayRaw, seasonWeekTagType, weekendDays)
            }

            usingMat.filter {
              case ((_, _, liveDt, _), _) =>
                liveDt >= futureStartDtNum && liveDt <= futureEndDtNum
            }.mapPartitions {
              par =>
                par.map {
                  case ((htlcd, segcd, liveDtNum, tag), row) =>

                    val liveDt = DateTime.parse(liveDtNum.toString, DateTimeFormat.forPattern("yyyyMMdd"))
                    val rowIdx = Days.daysBetween(fcDate, liveDt).getDays
                    val colIdx = advBkPeriod2ColIdx(rowIdx, matrixIdx2ColsMap)
                    val liveDtStr = liveDt.toString("yyyy-MM-dd")
                    //println(s"~$htlcd#$segcd#${liveDt.toString("yyyy-MM-dd")}#$tag#$colIdx#${row.mkString("#")}")
                    ((htlcd, segcd, tag), (liveDtStr, colIdx, row))
                }
            }
          }

          val parFutureTagMatWithMinColIdx = if (pairRDDReparNum > 0) futureTagMatWithMinColIdx.partitionBy(new HashPartitioner(pairRDDReparNum)) else futureTagMatWithMinColIdx


          /*println("futureTagMatWithMinColIdx = ")
          futureTagMatWithMinColIdx.collect().foreach{
            case ((htlcd, segcd, liveDt, tag, colIdx), row) =>
              println (s"$htlcd#$segcd#${if (liveDt != null) liveDt.toString("yyyy-MM-dd") else "NULL"}#$tag#$colIdx#${row.mkString("#")}")
          }*/

          val histTagRowsMap = if (testOpen) testHistTagRowsMap.get
          else {
            val histStartDt = fcDate.minusDays(defaultHistDays)
            val histEndDt = fcDate
            val histStartDtNum = histStartDt.toString("yyyyMMdd").toInt
            val histEndDtNum = histEndDt.toString("yyyyMMdd").toInt
            val dailyHistMat = dailyEntireMat.get.filter {
              case ((_, _, liveDtNum), _) =>
                liveDtNum >= histStartDtNum && liveDtNum <= histEndDtNum
            }
            val map = genOrigTagMat(dailyHistMat, seasonRaw, holidayRaw, seasonWeekTagType, weekendDays).mapPartitions {
              par =>
                par.map {
                  case ((htlcd, segcd, liveDtNum, usingTag), row) =>
                    ((htlcd, segcd, usingTag), row)
                }
            }
            val parMap = if (pairRDDReparNum > 0) map.partitionBy(new HashPartitioner(pairRDDReparNum)) else map
            parMap.groupByKey().cache()
          }


          val res = parFutureTagMatWithMinColIdx.leftOuterJoin(histTagRowsMap).mapPartitions {
            par =>
              par.map {
                case ((htlcd, segcd, tag), ((liveDtStr, minColIdx, currRow), histRowsOpt)) =>
                  val row = currRow.clone()
                  //val liveDtStr = liveDt.toString("yyyy-MM-dd")
                  val newRow = if(histRowsOpt.isDefined){
                    val histRows = histRowsOpt.get.toArray
                    val allRows = row +: histRows
                    val allRowsWithIdx = allRows.zipWithIndex
                    val allKnnFeatures = allRows.map {
                      arr =>
                        arr.slice(minColIdx, row.length)
                    }

                    val filteredDistanceIdxList = KNN.searchIndex(allKnnFeatures, knnK)
                    val filteredIdxList = filteredDistanceIdxList.map(_._2)

                    val filteredHistRows = allRowsWithIdx.filter {
                      case (_, idx) =>
                        filteredIdxList.contains(idx)
                    }.map(_._1)

                    (minColIdx - 1 to 0 by -1).foreach {
                      cIdx =>
                        val trainData = filteredHistRows.map {
                          r =>
                            val feature = r.slice(cIdx + 1, cIdx + 1 + m) :+ 1.0
                            val label = r(cIdx)
                            (label, feature)
                        }
                        val w = regressWithMatrix((htlcd, segcd, tag, cIdx), trainData, m, singularWArr)
                        if (printW) {
                          val msg = s"w#$htlcd#$segcd#$liveDtStr#$tag#$cIdx#${w.toArray.mkString("#")}"
                          logger.info(msg)
                          println(msg)
                        }

                        val predFeatures = DenseVector(row.slice(cIdx + 1, cIdx + 1 + m) :+ 1.0)
                        val pred = (predFeatures :* w).toArray.sum
                        row.update(cIdx, pred)
                    }
                    row.map(a => if (a < 0) -math.round(math.abs(a)) else math.round(a))
                  } else {
                    logger.warn(s"Not found hist rows of ($htlcd, $segcd, $tag)")
                    (minColIdx - 1 to 0 by -1).foreach {
                      cIdx =>
                        val w = singularVector
                        val predFeatures = DenseVector(row.slice(cIdx + 1, cIdx + 1 + m) :+ 1.0)
                        val pred = (predFeatures :* w).toArray.sum
                        row.update(cIdx, pred)
                    }
                    row.map(a => if (a < 0) -math.round(math.abs(a)) else math.round(a))
                  }

                  s"$fcDateStr#$htlcd#$segcd#$liveDtStr#${newRow.mkString("#")}"
              }
          }


          if (!testOpen && saveToFile) {
            val distPath = distDir + s"/dt=$fcDateSimpStr"
            if (distDir.startsWith("hdfs://")) {
              HDFSUtil.delete(hadoopHost, distPath)
            } else {
              val path: Path = Path(distPath)
              path.deleteRecursively()
            }
            res.saveAsTextFile(distPath)
          }
          res
      }

      if (testOpen && saveToFile) {
        val distPath = distDir
        if (distDir.startsWith("hdfs://")) {
          HDFSUtil.delete(hadoopHost, distPath)
        } else {
          val path: Path = Path(distPath)
          path.deleteRecursively()
        }

        if (results.nonEmpty) {
          var outputRDD = results(0)
          (1 until results.length).foreach {
            idx =>
              val currRDD = results(idx)
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
      println("Short term finished")
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
}
