package cn.jw.rms.ab.pred.split

import cn.jw.rms.data.framework.common.rules.PredictModelAssembly
import cn.jw.rms.data.framework.common.utils.HDFSUtil
import com.typesafe.config.Config
import org.apache.log4j.Logger
import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.rdd.RDD
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, Days}

import scala.util.{Failure, Random, Success, Try}
import collection.JavaConversions._
import scala.reflect.io.Path


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
      println("Split started")
      val combineDir = config.getString("nosplit-dir")
      val inventoryDir = config.getString("inventory-dir")
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
      val usingHtlTotal = config.getBoolean("htltotal-enable")
      val testOpen = config.getBoolean("hist-enable")
      val matrixCols = config.getIntList("matrix-columns").map(_.toInt).toArray
      val fcDateStr = config.getString("fc-date")
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
      val segConfRaw = sc.textFile(segpredconfDir).filter(_.nonEmpty).map(_.split(fieldSplitter, -1))
      val predSegConfRaw = segConfRaw.filter {
        r =>
          if (testOpen) {
            val segcd = r(segPredConfColIdx("seg_cd"))
            val isPredict = r(segPredConfColIdx("is_predict"))
            if (usingHtlTotal)
              isPredict == "Y" && segcd == "HTL_TOTAL"
            else
              isPredict == "Y" && segcd != "HTL_TOTAL"
          } else {
            r(segPredConfColIdx("is_predict")) == "Y"
          }
      }
      val predSegConfDistinct = predSegConfRaw.filter {
        r =>
          hotelCds.contains(r(segPredConfColIdx("htl_cd")))
      }.mapPartitions {
        par =>
          par.map {
            r =>
              val htlcd = r(segPredConfColIdx("htl_cd"))
              val segcd = r(segPredConfColIdx("seg_cd"))
              (htlcd, segcd)
          }
      }.distinct().collect()

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

      val startDt = fcDatePair._1
      val endDt = fcDatePair._2
      val fcDays = Days.daysBetween(startDt, endDt).getDays

      if (testOpen) {
        process(combineDir, inventoryDir, distDir)
      } else {
        (0 to fcDays).foreach {
          day =>
            val fcDate = startDt.plusDays(day)
            val fcDateSimpStr = fcDate.toString("yyyyMMdd")
            logger.info(s"fcDate = $fcDateSimpStr")
            val partitionName = "/dt=" + fcDateSimpStr
            val combinePath = combineDir + partitionName
            val inventoryPath = inventoryDir + partitionName
            val distPath = distDir + partitionName
            process(combinePath, inventoryPath, distPath)
        }
      }

      /*
        kernel method
      */
      def process(combinePath: String, inventoryPath: String, distPath: String) = {
        // get combine result
        val combineResultRaw = if (RDDReparNum > 0)
          sc.textFile(combinePath).filter(_.nonEmpty).map(_.split(fieldSplitter, -1)).repartition(RDDReparNum)
        else
          sc.textFile(combinePath).filter(_.nonEmpty).map(_.split(fieldSplitter, -1))

        val notFixCombineResult =
          combineResultRaw.filter {
            r =>
              val htlcd = r(matResultColIdx("htl_cd"))
              val segcd = r(matResultColIdx("seg_cd"))
              !predSegConfDistinct.contains((htlcd, segcd))
          }.mapPartitions{
            par =>
              par.map{
                r =>
                  val htlcd = r(matResultColIdx("htl_cd"))
                  val segcd = r(matResultColIdx("seg_cd"))
                  val fcdtStr = r(matResultColIdx("fc_dt"))
                  val lvdtStr = r(matResultColIdx("live_dt"))
                  val fcdt = DateTime.parse(fcdtStr, DateTimeFormat.forPattern("yyyy-MM-dd"))
                  val lvdt = DateTime.parse(lvdtStr, DateTimeFormat.forPattern("yyyy-MM-dd"))
                  val advFcDays = Days.daysBetween(fcdt, lvdt).getDays - 1
                  val startIdx = advBkPeriod2ColIdx(advFcDays, matrixIdx2ColsMap) + 1
                  val enableValues = r.slice(4 + startIdx, r.length).map(_.toInt)
                  val nullValues = Array.ofDim[Int](startIdx)
                  val values = Array.concat(nullValues, enableValues).map(_.toString)
                  fcdtStr +: htlcd +: segcd +: lvdtStr +: values
              }
          }

        val fixCombineResult = {
          combineResultRaw.filter {
            r =>
              val htlcd = r(matResultColIdx("htl_cd"))
              val segcd = r(matResultColIdx("seg_cd"))
              predSegConfDistinct.contains((htlcd, segcd))
          }.mapPartitions {
            par =>
              par.map {
                r =>
                  val htlcd = r(matResultColIdx("htl_cd"))
                  val segcd = r(matResultColIdx("seg_cd"))
                  val fcdt = r(matResultColIdx("fc_dt"))
                  val lvdt = r(matResultColIdx("live_dt"))
                  val values = r.slice(4, r.length).map(_.toInt)
                  ((fcdt, htlcd, segcd, lvdt), values)
              }
          }
        }

        /*println("------->fixCombineResult:")
        fixCombineResult.collect().foreach{
          case ((fcdt, htlcd, segcd, lvdt), values) =>
            println(s"$fcdt#$htlcd#$lvdt#$segcd#${values.mkString("#")}")
        }*/

        val parFixCombineResult = if (pairRDDReparNum > 0) fixCombineResult.partitionBy(new HashPartitioner(pairRDDReparNum)) else fixCombineResult

        val htlSumRns = parFixCombineResult.mapPartitions {
          par =>
            par.map {
              case ((fcdt, htlcd, segcd, lvdt), values) =>
                ((fcdt, htlcd, lvdt), values.sum)
            }
        }.reduceByKey(_ + _)

        /*    println("htlSumRns")
            htlSumRns.collect().foreach {
              case ((fcdt, htlcd, lvdt), sumRns) =>
                val str = s"$fcdt#$htlcd#$lvdt#$sumRns"
                println(str)
            }*/

        val htlSumRnsWithSeg = htlSumRns.mapPartitions {
          par =>
            par.flatMap {
              case ((fcdt, htlcd, lvdt), htlLvdtRns) =>
                predSegConfDistinct.filter(_._1 == htlcd).map {
                  case (_, segcd) =>
                    ((fcdt, htlcd, segcd, lvdt), htlLvdtRns)
                }
            }
        }

        /*println("htlSumRnsWithSeg:")
        htlSumRnsWithSeg.collect().foreach{
          case ((fcdt, htlcd, segcd, lvdt), htlLvdtRns) =>
            println(s"($fcdt, $htlcd, $segcd, $lvdt), $htlLvdtRns")
        }*/

        // get inventory
        val inventories = (if (RDDReparNum > 0)
          sc.textFile(inventoryPath).filter(_.nonEmpty).map(_.split(fieldSplitter, -1)).repartition(RDDReparNum)
        else
          sc.textFile(inventoryPath).filter(_.nonEmpty).map(_.split(fieldSplitter, -1))).filter {
          r =>
            val startDtNum = startDt.toString("yyyyMMdd").toInt
            val endDtNum = endDt.plusDays(fcDaysCount).toString("yyyyMMdd").toInt
            val lvDtNum = r(matResultColIdx("live_dt")).replaceAll("-", "").toInt
            hotelCds.contains(r(matResultColIdx("htl_cd"))) && lvDtNum >= startDtNum && lvDtNum <= endDtNum
        }.map {
          r =>
            val inv = r.slice(4, r.length).map(_.toInt).sum
            ((r(matResultColIdx("fc_dt")), r(matResultColIdx("htl_cd")), r(matResultColIdx("live_dt"))), inv)
        }

        val parInventories = if (pairRDDReparNum > 0) inventories.partitionBy(new HashPartitioner(pairRDDReparNum)) else inventories

        /*parInventories.collect().foreach {
          case ((fc_dt, htl, lvdt), inv) =>
            println(s"$fc_dt#$htl#$lvdt#$inv")
        }*/

        val htlSumInv = parInventories.mapPartitions {
          par =>
            par.map {
              case ((fcdt, htlcd, lvdt), htlInv) =>
                ((fcdt, htlcd, lvdt), htlInv)

            }
        }

        val htlSumInvWithSeg = htlSumInv.mapPartitions {
          par =>
            par.flatMap {
              case ((fcdt, htlcd, lvdt), htlInv) =>
                predSegConfDistinct.filter(_._1 == htlcd).map {
                  case (_, segcd) =>
                    ((fcdt, htlcd, segcd, lvdt), htlInv)
                }
            }
        }

        val fixResult = parFixCombineResult.join(htlSumRnsWithSeg).leftOuterJoin(htlSumInvWithSeg).mapPartitions {
          par =>
            par.map {
              case ((fcdtStr, htlcd, segcd, lvdtStr), ((row, htlRns), htlInvOpt)) =>
                val fixedRow = if (htlInvOpt.isDefined) {
                  val fcdt = DateTime.parse(fcdtStr, DateTimeFormat.forPattern("yyyy-MM-dd"))
                  val lvdt = DateTime.parse(lvdtStr, DateTimeFormat.forPattern("yyyy-MM-dd"))
                  val rowIdx = Days.daysBetween(fcdt, lvdt).getDays - 1
                  val totalInventory = htlInvOpt.get
                  val totalLvdtRns = htlRns
                  val segLvdtRns = row.sum
                  val segInventoryTmp = segLvdtRns.toDouble * totalInventory / totalLvdtRns.toDouble
                  val segInventory = if (segInventoryTmp >= 0) math.round(segInventoryTmp).toInt else -math.round(math.abs(segInventoryTmp)).toInt
                  logger.info(s"$fcdtStr#$lvdtStr#$htlcd#$segcd#segInventory = $segInventory#segLvdtRns = $segLvdtRns")
                  if (segLvdtRns != segInventory) {
                    val curRow = row.clone()
                    val otb1stColIdx = advBkPeriod2ColIdx(rowIdx, matrixIdx2ColsMap) + 1
                    val otbRns = curRow.slice(otb1stColIdx, curRow.length).sum
                    val deltaRns = segLvdtRns - segInventory
                    logger.info(s"$fcdtStr#$lvdtStr#totalInventory = $totalInventory#totalLvdtRns = $totalLvdtRns#deltaRns = $deltaRns#segLvdtRns = $segLvdtRns#segInventory = $segInventory#otb1stColIdx = $otb1stColIdx#otbRns = $otbRns")
                    if (segInventory >= otbRns) {
                      smoothingInventoryMoreThanOTB(deltaRns, otb1stColIdx, curRow)
                    } else {
                      smoothingInventoryLessThanOTB(segInventory, otbRns, deltaRns, otb1stColIdx, curRow)
                    }
                  } else row
                } else {
                  logger.warn(s"fcdt[$fcdtStr], htlcd[$htlcd], segcd[$segcd], lvdt[$lvdtStr] inventory is empty")
                  row
                }
                ((fcdtStr, htlcd, segcd, lvdtStr), fixedRow)
              //s"$fcdtStr#$htlcd#$segcd#$lvdtStr#" + fixedRow.mkString(fieldSplitter)
            }
        }.cache()

        /*println("------>fixResult:")
        fixResult.collect().foreach{
          case ((fcdtStr, htlcd, segcd, lvdtStr), fixedRow) =>
            println(s"$fcdtStr#$htlcd#$lvdtStr#$segcd#${fixedRow.mkString("#")}")
        }*/

        val fixedSumRns = fixResult.mapPartitions {
          par =>
            par.map {
              case ((fcdtStr, htlcd, segcd, lvdtStr), fixedRow) =>
                ((fcdtStr, htlcd, lvdtStr), fixedRow.sum)
            }
        }.reduceByKey(_ + _)

        /*println("----->fixedSumRns:")
        fixedSumRns.collect().foreach{
          case ((fcdtStr, htlcd, lvdtStr), sum) =>
            println(s"$fcdtStr#$htlcd#$lvdtStr#$sum")
        }

        println("----->htlSumInv:")
        htlSumInv.collect().foreach{
          case ((fcdt, htlcd, lvdt), htlInv) =>
            println(s"$fcdt#$htlcd#$lvdt#$htlInv")
        }*/


        //val predHtlSegMap = predSegConfDistinct.groupBy(_._1)
        val predHtlSegMap = fixResult.keys.mapPartitions{
          par =>
            par.map{
              case (fcdt, htlcd, segcd, lvdt) =>
                ((fcdt, htlcd, lvdt), segcd)
            }
        }.distinct().groupByKey().collectAsMap()

        /*println("------>predHtlSegMap")
        predHtlSegMap.foreach{
          case ((fcdt, htlcd, lvdt), segcd) =>
            println(s"$fcdt#$htlcd#$lvdt#$segcd")
        }*/

        val randSplitMap = fixedSumRns.leftOuterJoin(htlSumInv).filter {
          case ((fcdtStr, htlcd, lvdtStr), (rns, invOpt)) =>
            val inv = if (invOpt.isDefined) invOpt.get else 0
            inv != rns
        }.mapPartitions {
          par =>
            par.map {
              case ((fcdtStr, htlcd, lvdtStr), (rns, invOpt)) =>
                val inv = if (invOpt.isDefined) invOpt.get else 0
                val htlSegList = predHtlSegMap((fcdtStr, htlcd, lvdtStr)).toArray
                val randIdx = Random.nextInt(htlSegList.length)
                val randSeg = htlSegList(randIdx)
                //println(s"$fcdtStr#$htlcd#$lvdtStr: inv = $inv, rns = $rns")
                ((fcdtStr, htlcd, randSeg, lvdtStr), inv - rns)
            }
        }.collectAsMap()

        /*println("------>randSplitMap:")
        randSplitMap.foreach{
          case ((fcdtStr, htlcd, randSeg, lvdtStr), diff) =>
            println(s"$fcdtStr#$htlcd#$randSeg#$lvdtStr#$diff")
        }*/

        val needRandSplitResult = fixResult.filter {
          case ((fcdtStr, htlcd, segcd, lvdtStr), _) =>
            randSplitMap.contains((fcdtStr, htlcd, segcd, lvdtStr))
        }.mapPartitions {
          par =>
            par.map {
              case ((fcdtStr, htlcd, segcd, lvdtStr), fixedRow) =>
                val diff = randSplitMap((fcdtStr, htlcd, segcd, lvdtStr))
                val headVal = fixedRow(0)
                fixedRow.update(0, headVal + diff)
                logger.info(s"Split to#$fcdtStr#$htlcd#$segcd#$lvdtStr#$headVal#${fixedRow(0)}")
                s"$fcdtStr#$htlcd#$segcd#$lvdtStr#" + fixedRow.mkString(fieldSplitter)
            }
        }

        val noNeedRandSplitResult = fixResult.filter {
          case ((fcdtStr, htlcd, segcd, lvdtStr), _) =>
            !randSplitMap.contains((fcdtStr, htlcd, segcd, lvdtStr))
        }.mapPartitions {
          par =>
            par.map {
              case ((fcdtStr, htlcd, segcd, lvdtStr), fixedRow) =>
                s"$fcdtStr#$htlcd#$segcd#$lvdtStr#" + fixedRow.mkString(fieldSplitter)
            }
        }

        val result = needRandSplitResult.union(noNeedRandSplitResult)

        if (saveToFile) {
          if (distDir.startsWith("hdfs://")) {
            HDFSUtil.delete(hadoopHost, distPath)
          } else {
            val path: Path = Path(distPath)
            path.deleteRecursively()
          }

          result.union(notFixCombineResult.map(_.mkString(fieldSplitter))).saveAsTextFile(distPath)
        }
      }
      println("Split finished")
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

  private def smoothingInventoryLessThanOTB(segInventory: Int, otbRns: Int, deltaRns: Int, otb1stColIdx: Int, currLvdtRow: Array[Int]) = {
    val w = (0 until otb1stColIdx).map {
      j =>
        if (j == 0) {
          (currLvdtRow(j) - segInventory + otbRns).toDouble / deltaRns.toDouble
        } else {
          currLvdtRow(j).toDouble / deltaRns.toDouble
        }
    }
    logger.info("LessThanOTB: w :" + w.mkString("#"))
    val minusRnsList = (0 until otb1stColIdx).map {
      j =>
        val tmp = w(j) * deltaRns
        if (tmp >= 0) math.round(tmp).toInt else -math.round(math.abs(tmp)).toInt
    }
    logger.info("LessThanOTB: minusRnsList :" + minusRnsList.mkString("#"))
    (0 until otb1stColIdx).foreach {
      j =>
        currLvdtRow(j) = currLvdtRow(j) - minusRnsList(j)
    }
    currLvdtRow
  }

  private def smoothingInventoryMoreThanOTB(deltaRns: Int, otb1stColIdx: Int, currLvdtRow: Array[Int]) = {
    var maxJ = -1 //must start with -1
    var maxSum = 0
    (0 until otb1stColIdx).foreach {
      j =>
        val sum = (0 to j).map(j => currLvdtRow(j)).sum
        if (deltaRns - sum > 0) {
          maxJ = j
          maxSum = sum
        }
    }
    logger.info("MoreThanOTB: deltaRns = " + deltaRns + ", maxJ = " + maxJ + ", maxSum = " + maxSum)
    val w = (0 to maxJ + 1).map {
      j =>
        if (j == maxJ + 1) {
          (deltaRns - maxSum).toDouble / deltaRns.toDouble
        } else {
          currLvdtRow(j).toDouble / deltaRns.toDouble
        }
    }
    logger.info("MoreThanOTB: w :" + w.mkString("#"))

    val minusRnsList = (0 to maxJ + 1).map {
      j =>
        val tmp = w(j) * deltaRns
        if (tmp >= 0) math.round(tmp).toInt else -math.round(math.abs(tmp)).toInt
    }
    logger.info("MoreThanOTB: minusRnsList :" + minusRnsList.mkString("#"))

    (0 to maxJ + 1).foreach {
      j =>
        currLvdtRow(j) = currLvdtRow(j) - minusRnsList(j)
    }
    currLvdtRow
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
}
