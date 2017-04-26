package cn.jw.rms.ab.pred.sum

import cn.jw.rms.data.framework.common.rules.PredictModelAssembly
import cn.jw.rms.data.framework.common.utils.HDFSUtil
import com.typesafe.config.Config
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.joda.time.{DateTime, Days}

import collection.JavaConversions._
import scala.collection.Map
import scala.reflect.io.Path
import scala.util.{Failure, Success, Try}


object SeriLogger extends Serializable {
  @transient lazy val logger = Logger.getLogger(getClass.getName)
}

class Predictor extends PredictModelAssembly with Serializable {

  import SeriLogger.logger

  var gotSucceed = false
  var errMsg = ""
  val DATE_FORMAT_STR = "yyyy-MM-dd"
  val DT_FORMAT_STR = "yyyyMMdd"

  override def accuracy(sc: SparkContext, config: Config, prevStepRDD: Option[RDD[String]]): String = {
    "Not Supported"
  }

  override def succeed: (Boolean, String) = {
    (gotSucceed, errMsg)
  }

  override def predict(sc: SparkContext, config: Config, prevStepRDD: Option[RDD[String]]): Option[RDD[String]] = {
    Try {
      println("Sum started")
      val testOpen = config.getBoolean("hist-enable")
      val testFcPriceInputPath = config.getString("hist-fc-price-dir")
      val saveToFile = config.getBoolean("save-result-to-file")
      val hdfsHost = config.getString("hadoop-host")
      val field_seporator = config.getString("field-splitter")
      val origin_output_path = config.getString("dist-dir")
      val origin_matrix_input_path = config.getString("matrix-dir")
      val origin_fc_price_input_path = config.getString("fc-price-dir")
      val SECOND_FORMAT_STR = "yyyy-MM-dd HH:mm:ss"
      val matrixCols = config.getIntList("matrix-columns").map(_.toInt).toArray
      val invtFieldConfigDir = config.getString("invt-field-conifg-dir")
      val inventoryDir = config.getString("inventory-dir")
      val fcDaysCount = config.getInt("fc-days")

      val invPercentDir = config.getString("fix-percent-dir")
      val invPercentMap: Map[String, Iterable[(String, String, String, String)]] = sc.textFile(invPercentDir).map(_.split(field_seporator)).map {
        line =>
          val htlCd = line(invPercentColIdx("htl_cd"))
          val totalInv = line(invPercentColIdx("total_inv"))
          val currentInv = line(invPercentColIdx("current_inv"))
          val startdt = line(invPercentColIdx("startdt"))
          val enddt = line(invPercentColIdx("enddt"))
          (htlCd, (totalInv, currentInv, startdt, enddt))
      }.groupByKey().collectAsMap()

      val invtFieldMap: Map[String, String] = sc.textFile(invtFieldConfigDir).filter(_.nonEmpty).map(_.split(field_seporator, -1)).map {
        r =>
          val htlcd = r(invtFieldConfigIdx("htl_cd"))
          val field = r(invtFieldConfigIdx("field"))
          (htlcd, field)
      }.collectAsMap()
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
        val output_path = origin_output_path
        val msg = "output_path = " + output_path
        println(msg)
        logger.info(msg)
        val matrix_input_path = origin_matrix_input_path
        //println("matrix_input_path = " + matrix_input_path)
        val fc_price_input_path = testFcPriceInputPath
        //println("fc_price_input_path = " + fc_price_input_path)
        val inventory_input_path = inventoryDir
        println("inventory_input_path = " + inventory_input_path)

        val forJoinPrice = getAvgPrice(sc, field_seporator, fc_price_input_path)
        val fc_occ = getRns(sc, field_seporator, matrix_input_path, matrixIdx2ColsMap)

        val startDtNum = startDt.toString("yyyyMMdd").replace("-", "").toInt
        val endDtNum = endDt.plusDays(fcDaysCount).toString("yyyyMMdd").replace("-", "").toInt

        val inventory = getInventory(
          sc, inventory_input_path, invtFieldMap,
          startDtNum, endDtNum, field_seporator,invPercentMap)

        val exceptionRns = getExceptionRns(fc_occ, inventory)

        val fixMaxRns = getFixMaxRns(fc_occ, exceptionRns)

        val update_dt = new DateTime().toString(SECOND_FORMAT_STR)
        val result = getRev(forJoinPrice, fc_occ, update_dt, fixMaxRns)

        if (!hdfsHost.equals("local")) {
          HDFSUtil.delete(hdfsHost, output_path)
        } else {
          val path: Path = Path(output_path)
          path.deleteRecursively()
        }

        if (saveToFile) {
          result.saveAsTextFile(output_path)
        }
      } else {
        (0 to fcDays).foreach {
          day =>
            val fc_date = startDt.plusDays(day)
            val output_path = addDt(origin_output_path, fc_date)
            val msg = "output_path = " + output_path
            println(msg)
            logger.info(msg)
            val matrix_input_path = addDt(origin_matrix_input_path, fc_date)
            //println("matrix_input_path = " + matrix_input_path)
            val fc_price_input_path = addDt(origin_fc_price_input_path, fc_date)
            //println("fc_price_input_path = " + fc_price_input_path)
            val inventory_input_path = addDt(inventoryDir, fc_date)
            println("inventory_input_path = " + inventory_input_path)
            val forJoinPrice = getAvgPrice(sc, field_seporator, fc_price_input_path)
            val fc_occ = getRns(sc, field_seporator, matrix_input_path, matrixIdx2ColsMap)

            val startDtNum = fc_date.toString("yyyyMMdd").replace("-", "").toInt
            val endDtNum = fc_date.plusDays(fcDaysCount).toString("yyyyMMdd").replace("-", "").toInt
            val inventory = getInventory(
              sc, inventory_input_path, invtFieldMap,
              startDtNum, endDtNum, field_seporator,invPercentMap)

            val exceptionRns = getExceptionRns(fc_occ, inventory)

            println("exceptionRns:")
            exceptionRns.foreach {
              case ((htl_cd, live_dt, order_dt, fc_date_str), (sum, rooms)) =>
                println(s"$htl_cd#$live_dt#$order_dt#$fc_date_str#$sum#$rooms")
            }

            val fixMaxRns = getFixMaxRns(fc_occ, exceptionRns)

            val update_dt = new DateTime().toString(SECOND_FORMAT_STR)
            val result = getRev(forJoinPrice, fc_occ, update_dt, fixMaxRns)

            if (!hdfsHost.equals("local")) {
              HDFSUtil.delete(hdfsHost, output_path)
            } else {
              val path: Path = Path(output_path)
              path.deleteRecursively()
            }

            if (saveToFile) {
              result.saveAsTextFile(output_path)
            }
        }
      }
      println("Sum finished")
    } match {
      case Success(res) =>
        gotSucceed = true
        None
      case Failure(e) =>
        gotSucceed = false
        println(e.toString)
        logger.error(e.toString)
        e.printStackTrace()
        None
    }
  }

  private def getAvgPrice(sc: SparkContext, field_seporator: String, path: String) = {
    val rawFcPrice = sc.textFile(path)
    rawFcPrice.map { line =>
      val fields = line.split(field_seporator, -1)
      val htl_cd = fields(colIndexFcPrice("htl_cd"))
      val seg_cd = fields(colIndexFcPrice("seg_cd"))
      val live_dt = fields(colIndexFcPrice("live_dt"))
      val adr = fields(colIndexFcPrice("adr")).toFloat
      (JoinKey(htl_cd, seg_cd, live_dt), adr)
    }
  }

  private def getRns(sc: SparkContext, field_seporator: String, path: String, matrixIdx2ColsMap: IndexedSeq[(Int, (Int, Int))]) = {
    val rawMatrix = sc.textFile(path)
    val maxColSize = matrixIdx2ColsMap.length
    rawMatrix.flatMap { line =>
      val fields = line.split(field_seporator, -1)
      val fc_date_str = fields(0)
      val htl_cd = fields(1)
      val seg_cd = fields(2)
      val live_dt_str = fields(3)
      val values = fields.slice(4, fields.length).map(_.toInt)
      val fc_date = new DateTime(fc_date_str)
      val live_dt = new DateTime(live_dt_str)
      val fc_num = Days.daysBetween(fc_date, live_dt).getDays
      val (tmpEnd, (startAdvDay, endAdvDay)) = advBkPeriod2ColIdxV2(fc_num, matrixIdx2ColsMap)
      //println(s"$fc_num, $end, ($startAdvDay, $endAdvDay)")
      val end = if(tmpEnd + 1 > maxColSize) maxColSize else tmpEnd + 1

      val res = for (column1 <- 0 until end) yield {
        val advanced_num = colIdx2LastAdvBkDays(column1, matrixIdx2ColsMap)
        val order_dt = live_dt.minusDays(advanced_num).toString(DATE_FORMAT_STR)
        var occ = 0
        for (column2 <- column1 until values.length) {
          occ += values(column2)
        }
        val sum = if (occ < 0) 0 else occ
        (JoinKey(htl_cd, seg_cd, live_dt_str), (fc_date_str, order_dt, sum))
      }

      val pad0Res = if (startAdvDay != endAdvDay) {
        val new_order_dt = live_dt.minusDays(fc_num - 1).toString(DATE_FORMAT_STR)
        val ordDtExisted = res.exists {
          case (_, (_, order_dt, _)) =>
            order_dt == new_order_dt
        }
        if (!ordDtExisted) {
          val res = (JoinKey(htl_cd, seg_cd, live_dt_str), (fc_date_str, new_order_dt, values.slice(end, values.length).sum))
          println(s"res = $res, $startAdvDay, $endAdvDay, $end")
          Some(res)
        } else None
      } else None


      if (pad0Res.isDefined)
        res :+ pad0Res.get
      else res
    }
  }

  private def getInventory(sc: SparkContext, inventoryPath: String,
                           invtFieldMap: Map[String, String],
                           startDtNum: Int,
                           endDtNum: Int, fieldSplitter: String,invPercentMap: Map[String, Iterable[(String, String, String, String)]]) = {
    sc.textFile(inventoryPath).filter(_.nonEmpty).map(_.split(fieldSplitter, -1)).filter {
      r =>
        val invDtNum = r(inventoryIdx("inv_date")).replaceAll("-", "").toInt
        invDtNum >= startDtNum && invDtNum <= endDtNum
    }.mapPartitions {
      par =>
        par.map {
          r =>
            val htlcd = r(inventoryIdx("htl_cd"))
            val invDate = r(inventoryIdx("inv_date"))
            val invtRoomsFiledName = if (invtFieldMap.contains(htlcd)) invtFieldMap(htlcd) else "tosell"
            val (totalinv, currentinv) = if (invPercentMap.contains(htlcd)) {
              val dtSet: Array[(String, String, String, String)] = invPercentMap(htlcd).toArray
              val betweenDay = dtSet.find {
                case (a, b, st, et) =>
                  st <= invDate && et >= invDate
              }
              val resultInv = if (betweenDay.isDefined)
                betweenDay.get
              else
                ("1", "1", "1", "1")
              (resultInv._1, resultInv._2)
            } else ("1", "1")
            val invpercent = currentinv.toFloat / totalinv.toFloat
            val rooms = r(inventoryIdx(invtRoomsFiledName)).toFloat
            val currentRooms = (rooms * invpercent).toInt
            ((htlcd, invDate), currentRooms)
//            val invtRoomsFiledName = if (invtFieldMap.contains(htlcd)) invtFieldMap(htlcd) else "tosell"
//            ((htlcd, r(inventoryIdx("inv_date"))), r(inventoryIdx(invtRoomsFiledName)).toFloat.toInt)
        }
    }.reduceByKey(_ + _)
  }

  private def getExceptionRns(segRnsRdd: RDD[(JoinKey, (String, String, Int))], inventoryRdd: RDD[((String, String), Int)]) = {
    val htlRns = segRnsRdd.mapPartitions {
      par =>
        par.map {
          case (key, (fc_date_str, order_dt_str, sum)) =>
            ((key.htl_cd, key.live_dt, order_dt_str, fc_date_str), sum)
        }
    }.reduceByKey(_ + _).mapPartitions {
      par =>
        par.map {
          case ((htl_cd, live_dt, order_dt, fc_date_str), sum) =>
            ((htl_cd, live_dt), (order_dt, fc_date_str, sum))
        }
    }

    /*    println("htlRns:")
        htlRns.collect().foreach{
          case ((htl_cd, live_dt), (fc_date_str, sum)) =>
            println(s"$htl_cd#$live_dt#$fc_date_str#$sum")
        }*/

    /*println("inventoryRdd:")
    inventoryRdd.collect().foreach{
      case ((htl_cd, live_dt),rooms) =>
        println(s"$htl_cd#$live_dt#$rooms")
    }*/

    htlRns.join(inventoryRdd).mapPartitions {
      par =>
        par.map {
          case ((htl_cd, live_dt), ((order_dt, fc_date_str, sum), rooms)) =>
            ((htl_cd, live_dt, order_dt, fc_date_str), (sum, rooms))
        }
    }.filter {
      case ((htl_cd, live_dt, order_dt, fc_date_str), (sum, rooms)) =>
        sum > rooms
    }.collectAsMap()
  }

  private def getFixMaxRns(segRnsRdd: RDD[(JoinKey, (String, String, Int))], exceptionRns: Map[(String, String, String, String), (Int, Int)]) = {
    val filterRdd = segRnsRdd.filter {
      case (key, (fc_date_str, order_dt_str, sum)) =>
        exceptionRns.contains((key.htl_cd, key.live_dt, order_dt_str, fc_date_str))
    }
    filterRdd.mapPartitions {
      par =>
        par.map {
          case (key, (fc_date_str, order_dt_str, sum)) =>
            ((key.htl_cd, key.live_dt, order_dt_str, fc_date_str), (key.seg_cd, sum))
        }
    }.groupByKey().mapPartitions {
      par =>
        par.map {
          case ((htl_cd, live_dt, order_dt, fc_dt), vlist) =>
            val maxSum = vlist.map(_._2).max
            val maxSegcd = vlist.filter {
              case (segcd, sum) =>
                sum == maxSum
            }.head._1
            val (newSum, rooms) = exceptionRns((htl_cd, live_dt, order_dt, fc_dt))
            val fixRns = newSum - rooms
            logger.info(s"Sum fix from#$htl_cd#$maxSegcd#$live_dt#$order_dt#$fc_dt#$maxSum#$newSum#$rooms")
            logger.info(s"Sum fix to  #$htl_cd#$maxSegcd#$live_dt#$order_dt#$fc_dt#${maxSum - fixRns}")
            ((fc_dt, htl_cd, maxSegcd, live_dt, order_dt), fixRns)
        }
    }.collectAsMap()
  }

  private def getRev(avgPrice: RDD[(JoinKey, Float)], rns: RDD[(JoinKey, (String, String, Int))], update_dt: String, fixMaxRnsMap: Map[(String, String, String, String, String), Int]) = {
    val rnsFixPart = rns.filter{
      case (JoinKey(htl_cd, seg_cd, live_dt_str), (fc_date_str, order_dt, sum)) =>
        fixMaxRnsMap.contains((fc_date_str, htl_cd, seg_cd, live_dt_str, order_dt))
    }.mapPartitions {
      par =>
        par.map {
          case (key, (fc_date_str, order_dt, sum)) =>
            val fixRns = fixMaxRnsMap((fc_date_str, key.htl_cd, key.seg_cd, key.live_dt, order_dt))
            (key, (fc_date_str, order_dt, sum - fixRns))
        }
    }

    /*println("rnsFixPart:")
    rnsFixPart.filter {
      case (key, (fc_date_str, order_dt_str, sum)) =>
        key.htl_cd == "DJSW000001" && key.live_dt == "2016-09-16"  && order_dt_str == "2016-09-16"
    }.collect().foreach{
      case (key, (fc_date_str, order_dt_str, sum)) =>
        println(s"${key.htl_cd}#${key.seg_cd}#${key.live_dt}#$order_dt_str#$fc_date_str#$sum")
    }*/

    println("fixMaxRnsMap:")
    fixMaxRnsMap.foreach {
      case  ((fc_dt, htl_cd, maxSegcd, live_dt, order_dt), fixRns) =>
        println(s"$htl_cd#$maxSegcd#$order_dt#$live_dt#$fc_dt#$fixRns")
    }

    val rnsNoFixPart = rns.filter {
      case (key, (fc_date_str, order_dt, sum)) =>
        !fixMaxRnsMap.contains((fc_date_str, key.htl_cd, key.seg_cd, key.live_dt, order_dt))
    }

    /*println("rnsNoFixPart:")
    rnsNoFixPart.filter {
      case (key, (fc_date_str, order_dt_str, sum)) =>
        key.htl_cd == "DJSW000001" && key.live_dt == "2016-09-16"  && order_dt_str == "2016-09-16"
    }.collect()foreach{
      case (key, (fc_date_str, order_dt_str, sum)) =>
      println(s"${key.htl_cd}#${key.seg_cd}#${key.live_dt}#$order_dt_str#$fc_date_str#$sum")
    }*/

    val finalRns = rnsFixPart.union(rnsNoFixPart)

    finalRns.leftOuterJoin(avgPrice).map { case (key, value) =>
      val htl_cd = key.htl_cd
      val seg_cd = key.seg_cd
      val live_dt = key.live_dt
      val fc_dt = value._1._1
      val order_dt = value._1._2
      val occ = value._1._3
      val para_typ = 1
      val adr = if (value._2.isDefined) value._2.get else 0.0f
      val rev = adr * occ
      s"$htl_cd#$para_typ#$seg_cd#$order_dt#$live_dt#$fc_dt#$occ#$rev#$update_dt"
    }
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

  def advBkPeriod2ColIdxV2(origAdvBkPeriod: Int, matrixIdx2ColsMap: IndexedSeq[(Int, (Int, Int))]) = {
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

    res.head
  }

  private def colIdx2LastAdvBkDays(colIdx: Int, matrixIdx2ColsMap: IndexedSeq[(Int, (Int, Int))]) = {
    matrixIdx2ColsMap.filter(_._1 == colIdx).head._2._1
  }

  private def colIndexFcPrice(colname: String): Int = colname match {
    case "htl_cd" => 0
    case "seg_cd" => 1
    case "is_member" => 2
    case "live_dt" => 3
    case "fc_dt" => 4
    case "adr" => 5
  }

  private def addDt(path: String, fc_date: DateTime): String = {
    if (path.endsWith("/")) {
      path + "dt=" + fc_date.toString(DT_FORMAT_STR)
    } else {
      path + "/dt=" + fc_date.toString(DT_FORMAT_STR)
    }
  }

  private def inventoryIdx(colName: String) = colName match {
    case "htl_cd" => 0
    case "observe_dt" => 1
    case "inv_date" => 2
    case "rm_type" => 3
    case "rooms" => 4
    case "extra" => 5
    case "ooo" => 6
    case "oos" => 7
    case "tosell" => 8
    case "ovb" => 9
    case "avl" => 10
    case "update_dt" => 11
  }

  private def invtFieldConfigIdx(colName: String) = colName match {
    case "htl_cd" => 0
    case "field" => 1
  }
  private def invPercentColIdx(colName: String) = colName match {
    case "htl_cd" => 0
    case "total_inv" => 1
    case "current_inv" => 2
    case "startdt" => 3
    case "enddt" => 4
  }
}
