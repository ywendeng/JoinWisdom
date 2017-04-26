package cn.jw.rms.pred.genconfig

import java.io.{File, PrintWriter}

import cn.jw.rms.data.framework.common.rules.CleanerAssembly
import com.typesafe.config.Config
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.joda.time.{DateTime, Days}

import org.joda.time.format.DateTimeFormat

import collection.JavaConversions._

import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.util.{Failure, Success, Try}


object SeriLogger extends Serializable {
  @transient lazy val logger = Logger.getLogger(getClass.getName)
}

class Cleaner extends CleanerAssembly with Serializable {

  import SeriLogger.logger

  var gotSucceed = false
  var errMsg = ""
  val V_HTL = "V_HTL"
  val HTLCD = "htlcd"
  val SEGCD = "segcd"
  val SEG_IS_PRED = "seg_is_pred"
  val SEASON = "season"
  val INVT_FIELD_CONFIG = "invt_field_config"
  val PREDCONF = "predconf"
  val IS_PREDICT = "is_predict"
  val PRICE_MODEL ="price_model"
  val HDFS_FLG = "hdfs://"

  override def succeed: (Boolean, String) = (gotSucceed, errMsg)

  override def clean(sc: SparkContext, config: Config, prevStepRDD: Option[RDD[String]]): Option[RDD[String]] = {
    Try {
      println("gen config started")
      val version = config.getString("version")
      val configDistDir = config.getString("config-dist-dir")
      val hotelList = config.getStringList("hotel-list")
      val filedSplitter = config.getString("filed-splitter")
      val simpleConfigList = config.getConfigList("simple-config-list").map(SimpleConf.apply)
      val complexConfigList = config.getConfigList("complex-config-list").map(ComplexConf.apply).sortBy(_.index)
      val curDtStr = DateTime.now.minusDays(1).toString("yyyyMMdd")
      val minInterval = config.getInt("minInterval")
      val seasonNo = config.getInt("seasonNo")
      val startDt = config.getString("startDt")
      val endDt = config.getString("endDt")
      val trainStart = config.getString("train-start")
      val trainEnddt = config.getString("train-end")
      val mysqlUrl = config.getString("db.url")
      val mysqlUser = config.getString("db.username")
      val mysqlPassword = config.getString("db.password")
      val predmysqlTable = config.getString("db.predtable")
      val pricemysqlTable = config.getString("db.pricetable")


      // process complex config
      complexConfigList.foreach {
        conf =>
          val name = conf.name
          val srcList = conf.srcList
          name match {
            case IS_PREDICT =>
              val content = processIsPred(sc,mysqlUrl, mysqlUser, mysqlPassword, predmysqlTable)
              val srcMap = srcList.map {
                src =>
                  (src.name, src.dir)
              }.toMap
              val segIsPredDist = srcMap(SEG_IS_PRED)
              hotelList.foreach {
                htl =>
                  val distPath = s"$configDistDir/$version/data/$htl/$name/data"
                  val localBasePath=segIsPredDist+"/data"
                  writeFile(distPath, content)
                  writeFile(localBasePath,content)
              }

            case PREDCONF =>
              val content = processPredConf(srcList, sc, filedSplitter)
              hotelList.foreach {
                htl =>
                  val distPath = s"$configDistDir/$version/data/$htl/$name/data"
                  writeFile(distPath, content)
              }

            case PRICE_MODEL =>
              val content = processPriceModel(sc,mysqlUrl, mysqlUser, mysqlPassword, pricemysqlTable)
              hotelList.foreach {
                htl =>
                  val distPath = s"$configDistDir/$version/data/$htl/$name/data"
                  writeFile(distPath, content)
              }
          }
      }

     println("------------------------------- \n complete the complex training \n ------------------------------")

      // process simple config
      simpleConfigList.foreach {
        conf =>
          val name = conf.name
          val srcDir = conf.srcDir
          val dtOpt = conf.dt
          val localPath = conf.localDir
          val backupPath = conf.backupDir

          val srcPath = if (dtOpt.isDefined && srcDir.startsWith(HDFS_FLG)) {
            val dtStr = if (dtOpt.get == "now") curDtStr else dtOpt.get
            s"$srcDir/dt=$dtStr"
          } else srcDir

          name match {
            case SEASON =>
              hotelList.foreach {
                htl =>
                  val content = processSeason(srcPath, htl, sc, minInterval, seasonNo, startDt, endDt, trainStart, trainEnddt, localPath,backupPath)
                  val distPath = s"$configDistDir/$version/data/$htl/$name/data"
                  writeFile(distPath, content)
              }
            case INVT_FIELD_CONFIG =>
              hotelList.foreach {
                htl =>
                  val content = processInvtFieldConfig(srcPath, htl, sc)
                  val distPath = s"$configDistDir/$version/data/$htl/$name/data"
                  writeFile(distPath, content)
              }
            case _ =>
              val content = processOthers(srcPath, sc)
              hotelList.foreach {
                htl =>
                  val distPath = s"$configDistDir/$version/data/$htl/$name/data"
                  writeFile(distPath, content)
              }
          }
      }

      println("gen config finished")
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

  private def processOthers(srcPath: String, sc: SparkContext) = {
    if (srcPath.startsWith(HDFS_FLG))
      sc.textFile(srcPath).filter(_.nonEmpty).collect().mkString("\n")
    else
      readFile(srcPath)
  }

  private def processInvtFieldConfig(srcTplPath: String, htl: String, sc: SparkContext) = {
    val src =
      if (srcTplPath.startsWith(HDFS_FLG))
        sc.textFile(srcTplPath).filter(_.nonEmpty).collect().mkString("\n")
      else readFile(srcTplPath)

    if (src.contains(htl)) src else s"$src\n$htl#rooms"
  }

  private def processSeason(srcTplPath: String, htl: String, sc: SparkContext, minInterval: Int, seasonNo: Int, startDt: String, endDt: String, trainStart: String, trainEnddt: String,localPath:Option[String],backupDir: Option[String]) = {

    val srcdata = sc.textFile(srcTplPath).filter(_.nonEmpty).map(_.split("#"))

    val noPredseg = readFileLines(localPath.get).map(_.split("#", -1)).map {
      r =>
        (r(0), r(1), r(2))
    }.distinct.filter {
      case (htlCd, segCd, isPred) =>
        isPred.equals("N") && htlCd == htl
    }.map(_._2)


    val liveDtSet = srcdata.filter {
      line =>
        val htlcd = line(segBkDailySumColIdx("htl_cd"))
        htlcd == htl
    }.map {
      r =>
        r(segBkDailySumColIdx("live_dt")).replaceAll("-","").toInt
//        DateTime.parse(liveDt, DateTimeFormat.forPattern("yyyy-MM-dd"))
    }


    val trainStartDt = trainStart.replaceAll("-","").toInt
    val minLiveDt = liveDtSet.min
    val minTrainDt = if (trainStartDt > minLiveDt) trainStartDt else minLiveDt

//    println("minTrainDt is :"+minTrainDt)
    val maxLiveDt = trainEnddt.replaceAll("-","")
//    println("maxLiveDt is :"+maxLiveDt)
    val minLiveTime = DateTime.parse(minTrainDt.toString, DateTimeFormat.forPattern("yyyyMMdd"))
    val maxLiveTime = DateTime.parse(maxLiveDt, DateTimeFormat.forPattern("yyyyMMdd"))

    val liveDtDiff = Days.daysBetween(minLiveTime, maxLiveTime).getDays



   if (liveDtDiff >= 365){
    val weekData = srcdata.filter {
      r =>
        val htlcd = r(segBkDailySumColIdx("htl_cd"))
        val liveDt = r(segBkDailySumColIdx("live_dt"))
        val liveDate = DateTime.parse(liveDt, DateTimeFormat.forPattern("yyyy-MM-dd")).getMillis
        val segCd = r(segBkDailySumColIdx("seg_cd"))
        htlcd == htl && liveDate >= minLiveTime.getMillis && !noPredseg.contains(segCd) && liveDate <= maxLiveTime.getMillis
    }.map {
      r =>
        val htlcd = r(segBkDailySumColIdx("htl_cd"))
        val liveDt = r(segBkDailySumColIdx("live_dt"))
        val rns = r(segBkDailySumColIdx("rns")).toDouble
        ((htlcd, liveDt), rns)
    }.reduceByKey(_ + _).map {
      case ((htlcd, liveDt), totalrns) =>
        val liveTime = DateTime.parse(liveDt, DateTimeFormat.forPattern("yyyy-MM-dd"))
        val weekOfYear = liveTime.getWeekOfWeekyear / minInterval
        //        println(s"liveDt:$liveDt  week:${liveTime.getWeekOfWeekyear} weekofYear:$weekOfYear")
        ((htlcd, weekOfYear), (totalrns, 1))
    }.reduceByKey {
      case (a, b) =>
        (a._1 + b._1, a._2 + b._2)
    }.map {
      case (((htlcd, weekOfYear), (rns, total))) =>
        val meanRns = rns / total
        ((htlcd, weekOfYear), meanRns.toFloat)
    }.collectAsMap()


    //    weekData.foreach(println)
    val maxRns = weekData.values.max
    //    println(s"maxRns:$maxRns")
    val minRns = weekData.values.min
    //    println(s"maxRns:$minRns")
    val seasonSet = weekData.map {
      case ((htlCd, weekOfyear), rns) =>
        val seasonId = getSeasonType(maxRns, minRns, seasonNo, rns)
        ((htlCd, weekOfyear), seasonId)
    }

    val startTime = DateTime.parse(startDt, DateTimeFormat.forPattern("yyyy-MM-dd"))
    val endTime = DateTime.parse(endDt, DateTimeFormat.forPattern("yyyy-MM-dd"))
    val firstWeekOfYear = startTime.getWeekOfWeekyear / minInterval
    var baseSeasonId = seasonSet(htl, firstWeekOfYear)
    var startdt = startDt
    var enddt = startDt
    val intervalDays = Days.daysBetween(startTime, endTime).getDays
    val result = (0 to intervalDays).map {
      day =>
        val lvDt = startTime.plusDays(day)
        val lvdt = lvDt.toString("yyyy-MM-dd")
        val currentWeek = lvDt.getWeekOfWeekyear / minInterval
        val currentSeasonId = seasonSet(htl, currentWeek)

        if (currentSeasonId != baseSeasonId) {
          val currentstartDt = startdt
          val oldBaseSeason = baseSeasonId
          startdt = lvdt
          baseSeasonId = currentSeasonId
          val distdata = s"htl#$$oldBaseSeason####$currentstartDt#$enddt"
          Some(distdata)
        } else {
          enddt = lvdt
          None
        }
    }
    val printResult = ArrayBuffer[String]()
    result.map {
      line =>
        if (line.isDefined) {
          printResult += line.get
        }
    }
    printResult += s"$htl#$baseSeasonId####$startdt#$enddt"
    printResult.distinct
    //    printResult.foreach(println)
    printResult.mkString("\n")
  }else
   {
    val backPath = backupDir.get
     val seasonDemo = readFileLines(backPath)
     seasonDemo.map{
       line=>
         line.replaceAll("V_HTL",htl)
     }.mkString("\n")
   }
  }

  private def getSeasonType(max_input: Float, min_input: Float, nbr: Int, Rns: Float) = {
    var seasonID = 0
    for (seasonId <- nbr to 1 by -1) {
      //      val result = (max_input - min_input) * seasonId / nbr + min_input

      if ((max_input - min_input) * seasonId / nbr + min_input + 0.0001 >= Rns)
        seasonID = seasonId
      //      println(s"result:$result seasonId:$seasonId Rns:$Rns seasonID:$seasonID")
    }
    seasonID
  }

  private def processPredConf(srcList: Array[ComplexSrc], sc: SparkContext, filedSplitter: String) = {
    val srcMap = srcList.map {
      src =>
        (src.name, src)
    }.toMap

    val segIsPredSrc = srcMap(SEG_IS_PRED)
    val segIsPredFieldIdx = segIsPredSrc.fieldList.zipWithIndex.toMap
    val segIsPredMap = (if (segIsPredSrc.dir.startsWith(HDFS_FLG))
      sc.textFile(segIsPredSrc.dir).filter(_.nonEmpty).collect().toList
    else
      readFileLines(segIsPredSrc.dir)).map(_.split(filedSplitter, -1)).map {
      r =>
        (r(segIsPredFieldIdx("htl_cd")), r(segIsPredFieldIdx("seg_cd")), r(segIsPredFieldIdx("is_pred")))
    }.distinct

    segIsPredMap.map {
      case (htlcd, segcd,isPred) =>
        s"#$htlcd#$segcd###$isPred###"
    }.mkString("\n")
  }

  private def processIsPred(sc:SparkContext,mysqlUrl: String, mysqluser: String, mysqlpassword: String, mysqltable: String) = {
    val sqlContext = new SQLContext(sc)
    //    val url = "jdbc:mysql://106.75.28.62:6869/rms"
    val url = mysqlUrl
    val jdbcDF = sqlContext.read.format("jdbc").options(
      Map("url" -> url,
        "user" -> mysqluser,
        "password" -> mysqlpassword,
        "dbtable" -> mysqltable)).load()
    jdbcDF.registerTempTable("rms_ref_market_seg")
    val result = sqlContext.sql("select htl_cd , seg_cd, is_forecast from rms_ref_market_seg")
    //    val result = sqlContext.sql("select * from rms_ref_market_seg ")
    result.collect().map { row =>
      (row(0).toString, row(1).toString, row(2).toString)
    }.map {
      case (htlcd, segcd, ispred) =>
        s"$htlcd#$segcd#$ispred"
    }.mkString("\n")
  }

  private def processPriceModel(sc:SparkContext,mysqlUrl: String, mysqluser: String, mysqlpassword: String, mysqltable: String) = {
    val sqlContext = new SQLContext(sc)
    //    val url = "jdbc:mysql://106.75.28.62:6869/rms"
    val url = mysqlUrl
    val jdbcDF = sqlContext.read.format("jdbc").options(
      Map("url" -> url,
        "user" -> mysqluser,
        "password" -> mysqlpassword,
        "dbtable" -> mysqltable)).load()
    jdbcDF.registerTempTable("hotel_bar_map")
    val result = sqlContext.sql("select htl_cd ,season,zh_rt_cd, min_occ_pert,max_occ_pert from hotel_bar_map")
    //    val result = sqlContext.sql("select * from rms_ref_market_seg ")
    result.collect().distinct.map{ row =>
      (row(0).toString, row(1).toString, row(2).toString,row(3).toString,row(4).toString)
    }.map {
      case (htlcd, segcd,zh_rt_cd, min_occ_pert,max_occ_pert) =>
        s"1#$htlcd#$segcd#$zh_rt_cd#$min_occ_pert#$max_occ_pert#"
    }.mkString("\n")
  }


  private def readFile(path: String) = {
    println(s"path = $path")
    val file = new File(path)
    val pathList = file.listFiles().filterNot(f => f.isHidden).map(_.getAbsolutePath)
    pathList.map {
      p =>
        println(s"p = $p")
        Source.fromFile(p).getLines().mkString("\n")
    }.mkString("\n")
  }

  private def readFileLines(path: String) = {
    val file = new File(path)
    val pathList = file.listFiles().filterNot(f => f.isHidden).map(_.getAbsolutePath)
    pathList.flatMap {
      p =>
        Source.fromFile(p).getLines()
    }.toList
  }

  private def writeFile(path: String, content: String) = {
    val file = new File(path)
    if (!file.exists()) {
      if (!file.getParentFile.exists())
        file.getParentFile.mkdirs()
      file.createNewFile()
    }
    val rootStartPw = new PrintWriter(file)
    rootStartPw.write(content)
    rootStartPw.close()
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
