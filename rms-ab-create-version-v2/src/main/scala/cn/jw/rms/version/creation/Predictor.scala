package cn.jw.rms.version.creation

import java.io.{File, PrintWriter}
import cn.jw.rms.data.framework.common.rules.PredictModelAssembly
import cn.jw.rms.data.framework.common.utils.HDFSUtil
import com.typesafe.config.Config
import org.apache.commons.io.FileUtils
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.joda.time.{DateTime, Days}
import org.joda.time.format.DateTimeFormat
import collection.JavaConversions._
import scala.io.Source
import scala.reflect.io.Path
import scala.util.{Failure, Success, Try}


object SeriLogger extends Serializable {
  @transient lazy val logger = Logger.getLogger(getClass.getName)
}

class Predictor extends PredictModelAssembly with Serializable {

  import SeriLogger.logger

  var gotSucceed = false
  var errMsg = ""
  val V_SF_TR_BEGIN_DT = "V_SF_TR_BEGIN_DT"
  val V_SF_TR_END_DT = "V_SF_TR_END_DT"
  val V_LF_TR_BEGIN_DT = "V_LF_TR_BEGIN_DT"
  val V_LF_TR_END_DT = "V_LF_TR_END_DT"
  val V_WSF_TR_BEGIN_DT = "V_WSF_TR_BEGIN_DT"
  val V_WSF_TR_END_DT = "V_WSF_TR_END_DT"
  val V_FC_DATE_LIST = "V_FC_DATE_LIST"
  val V_ROOT_DIR = "V_ROOT_DIR"
  val V_VERSION = "V_VERSION"
  val V_APP_CONF = "V_APP_CONF"
  val V_HTL = "V_HTL"
  val SHELL_START_LINE = "#!/bin/bash"
  val PREDCONF = "predconf"
  val SEASON = "season"

  override def accuracy(sc: SparkContext, config: Config, prevStepRDD: Option[RDD[String]]): String = "Not Supported"

  override def succeed: (Boolean, String) = (gotSucceed, errMsg)

  override def predict(sc: SparkContext, config: Config, prevStepRDD: Option[RDD[String]]): Option[RDD[String]] = {
    Try {
      val rootVersion = config.getString("root-version")
      println(s"Create version [$rootVersion] started.")
      val tplRootDir = config.getString("template-root-dir")
      val confRootDir = config.getString("conf-root-dir")
      val distRootDir = config.getString("dist-root-dir")
      val histDataDir = config.getString("hist-data-dir")
      val staticKVList: Seq[KV] = config.getConfigList("static-kv-list").map(KV.apply)
      val trainRatio = config.getDouble("train-ratio")
      val testRatio = config.getDouble("test-ratio")
      val fieldSplitter = config.getString("field-splitter")
      val wsfweightDays = config.getInt("wsfweight-days")
      val hadoopHost = config.getString("hadoop-host")
      val uploadConfEnable = config.getBoolean("upload-conf-enable")

      val htlList = getHtlList(tplRootDir, rootVersion)
      val htlTotal = htlList.length
      val parallelismTmp = config.getInt("parallelism")
      val parallelism = if (parallelismTmp > htlTotal) htlTotal else parallelismTmp
      val quotient = htlTotal / parallelism
      val remainder = htlTotal % parallelism
      val htlUnitListTmp = (0 until parallelism).map {
        idx =>
          val startIdx = idx * quotient
          val endIdx = startIdx + quotient
          htlList.slice(startIdx, endIdx)
      }

      val htlUnitList = if (remainder > 0) {
        val leftHtls = htlList.slice(htlList.length - remainder, htlList.length)
        var idx = 0
        htlUnitListTmp.map {
          arr =>
            val res = if (idx < leftHtls.length) leftHtls(idx) +: arr else arr
            idx += 1
            res
        }
      } else htlUnitListTmp

      val htlMinMaxLvDtMap = getHtlMinMaxLiveDtMap(sc, histDataDir, fieldSplitter)
      println("Finished getHtlMinMaxLiveDtMap")
      val dateInfoMap = genDateInfoMap(trainRatio, testRatio, htlMinMaxLvDtMap, wsfweightDays)
      distributeProgram(tplRootDir, rootVersion, distRootDir, htlUnitList, staticKVList, trainRatio, dateInfoMap)
      println("Finished distributeProgram")
      if (uploadConfEnable) {
        upload2hdfs(sc, tplRootDir, rootVersion, confRootDir, htlList, hadoopHost, fieldSplitter)
        println("Finished upload2hdfs")
      }
      /*println("htlList")
      htlList.foreach(println)

      println("htlUnitList")
      htlUnitList.foreach{
        arr =>
          println("\"" + arr.mkString("\",\"") + "\"")
      }*/

      println(s"Create version [$rootVersion] finished.")
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

  private def getHtlMinMaxLiveDtMap(sc: SparkContext, histDataDir: String, fieldSplitter: String) = {
    val histData = sc.textFile(histDataDir).filter(_.nonEmpty).map(_.split(fieldSplitter, -1))
    histData.mapPartitions {
      par =>
        par.map {
          r =>
            val htlcd = r(srcColIdx("htl_cd"))
            val lvdtStr = r(srcColIdx("live_dt"))
            val lvdtNum = lvdtStr.replaceAll("-", "").toInt
            (htlcd, (lvdtNum, lvdtNum))
        }
    }.reduceByKey {
      case ((n1, n2), (n3, n4)) =>
        (math.min(n1, n3), math.max(n2, n4))
    }.mapPartitions {
      par =>
        par.map {
          case (htlcd, (minNum, maxNum)) =>
            val minDtStr = DateTime.parse(minNum.toString, DateTimeFormat.forPattern("yyyyMMdd")).toString("yyyy-MM-dd")
            val maxDtStr = DateTime.parse(maxNum.toString, DateTimeFormat.forPattern("yyyyMMdd")).toString("yyyy-MM-dd")
            (htlcd, (minDtStr, maxDtStr))
        }
    }.collectAsMap()
  }

  private def getHtlList(tplRootDir: String, rootVersion: String) = {
    val dataPath = s"$tplRootDir/$rootVersion/data/"
    listDirs(dataPath)
  }

  private def listDirs(directoryName: String): Array[String] = {
    new File(directoryName).listFiles.filter(_.isDirectory).map(_.getName)
  }

  private def upload2hdfs(sc: SparkContext, tplRootDir: String, rootVersion: String,
                          confRootDir: String, htlList: Array[String], hadoopHost: String,
                          fieldSplitter: String) = {

    var idx = 0
    htlList.foreach {
      htl =>
        val version = rootVersion
        val htlDataPath = s"$tplRootDir/$rootVersion/data/$htl/"
        val dataNameList = listDirs(htlDataPath)
        dataNameList.foreach {
          dataName =>
            val srcPath = s"$htlDataPath/$dataName/"
            val fileContent = readFileLines(srcPath)
            val srcData = sc.parallelize(fileContent).filter(_.nonEmpty)
            val distPath = if (dataName == PREDCONF || dataName == SEASON)
              s"$confRootDir/$version/$dataName/$htl/"
            else
              s"$confRootDir/$version/$dataName"

            if (idx > 0) {
              if (dataName == PREDCONF || dataName == SEASON){
                if (confRootDir.startsWith("hdfs://")) {
                  HDFSUtil.delete(hadoopHost, distPath)
                } else {
                  val path: Path = Path(distPath)
                  path.deleteRecursively()
                }
                srcData.saveAsTextFile(distPath)
                println(s"Finished upload data [$htl][$dataName]")
              }
            } else {
              if (confRootDir.startsWith("hdfs://")) {
                HDFSUtil.delete(hadoopHost, distPath)
              } else {
                val path: Path = Path(distPath)
                path.deleteRecursively()
              }
              srcData.saveAsTextFile(distPath)
              println(s"Finished upload data [$htl][$dataName]")
            }
        }
        idx += 1
    }
  }

  private def distributeProgram(tplRootDir: String, rootVersion: String,
                                distRootDir: String, htlUnitList: IndexedSeq[Array[String]],
                                staticKVList: Seq[KV], trainRatio: Double,
                                dateInfoMap: collection.Map[String, (String, String, String, String, String, String)]) = {
    val srcProgramPath = s"$tplRootDir/$rootVersion/program"
    val src = new File(srcProgramPath)
    /*val rootDist = new File(distRootDir)
    FileUtils.cleanDirectory(rootDist)*/
    var idx = 0
    htlUnitList.foreach {
      htlList =>
        idx = idx + 1
        val distPath = s"$distRootDir/$rootVersion" + "_" + s"$idx/"
        val dist = new File(distPath)
        FileUtils.deleteDirectory(dist)
        FileUtils.copyDirectory(src, dist)
        val rootStartPath = s"$distPath/start.sh"
        val rootShellSb = new StringBuilder
        rootShellSb.append(s"$SHELL_START_LINE\n")
        val confTplPath = s"$distPath/conf/application.conf"
        val startTplPath = s"$distPath/start.sh"

        val takeOneHtl = htlList.head
        val htlSet = htlList.mkString(",")
        val version = rootVersion
        //test code
        /*val (wsfStart, wsfEnd, trainStart, trainEnd, testStart, testEnd) =
          if(dateInfoMap.contains(htl)) dateInfoMap(htl)
          else ("NoData", "NoData", "NoData", "NoData", "NoData", "NoData")*/
        val (wsfStart, wsfEnd, trainStart, trainEnd, testStart, testEnd) = dateInfoMap(takeOneHtl)

        var confTplContent = Source.fromFile(confTplPath).getLines().mkString("\n")
        var startTplContent = Source.fromFile(startTplPath).getLines().mkString("\n")

        //replace static parameters
        staticKVList.foreach {
          kv =>
            confTplContent = confTplContent.replace(kv.name, kv.value)
            startTplContent = startTplContent.replace(kv.name, kv.value)
        }

        //replace date list
        confTplContent = confTplContent.replace(V_WSF_TR_BEGIN_DT, wsfStart)
        confTplContent = confTplContent.replace(V_WSF_TR_END_DT, wsfEnd)
        confTplContent = confTplContent.replace(V_SF_TR_BEGIN_DT, trainStart)
        confTplContent = confTplContent.replace(V_SF_TR_END_DT, trainEnd)
        confTplContent = confTplContent.replace(V_LF_TR_BEGIN_DT, trainStart)
        confTplContent = confTplContent.replace(V_LF_TR_END_DT, trainEnd)
        val fcDtList = s"$testStart,$testEnd"
        confTplContent = confTplContent.replace(V_FC_DATE_LIST, fcDtList)
        //replace others
        confTplContent = confTplContent.replace(V_ROOT_DIR, distPath)
        startTplContent = startTplContent.replace(V_ROOT_DIR, distPath)
        confTplContent = confTplContent.replace(V_VERSION, version)
        confTplContent = confTplContent.replace(V_HTL, htlSet)
        val confName = s"application_$version.conf"
        val startName = s"start_$version.sh"
        startTplContent = startTplContent.replace(V_APP_CONF, confName)

        //write start file
        val startPath = s"$distPath/$startName"
        val startPw = new PrintWriter(new File(startPath))
        startPw.write(startTplContent)
        startPw.close()
        //write conf file
        val confPw = new PrintWriter(new File(s"$distPath/conf/$confName"))
        confPw.write(confTplContent)
        confPw.close()
        rootShellSb.append(s"sh -x $startPath\n")
        println(s"Finished conf and start of [$htlSet]")

        val rootStartPw = new PrintWriter(new File(rootStartPath))
        rootStartPw.write(rootShellSb.toString)
        rootStartPw.close()
        FileUtils.deleteQuietly(new File(confTplPath))
        println(s"Finished conf and start of unit [$idx]")
    }
  }

  private def genDateInfoMap(trainRatio: Double, testRatio: Double, htlMinMaxLiveDtMap: collection.Map[String, (String, String)], wsfweightDays: Int) = {
    val currDate = DateTime.parse(DateTime.now.minusDays(1).toString("yyyy-MM-dd"), DateTimeFormat.forPattern("yyyy-MM-dd"))
    htlMinMaxLiveDtMap.map {
      case (htl, (minDtStr, maxDtStr)) =>
        val minDt = DateTime.parse(minDtStr, DateTimeFormat.forPattern("yyyy-MM-dd"))
        val tmpMaxDt = DateTime.parse(maxDtStr, DateTimeFormat.forPattern("yyyy-MM-dd"))
        val maxDt = if (currDate.getMillis < tmpMaxDt.getMillis) currDate else tmpMaxDt
        val dayCount = Days.daysBetween(minDt, maxDt).getDays
        val trainDayCount = (dayCount * trainRatio).toInt
        val testDayCount = (dayCount * testRatio).toInt
        val trainStartDt = minDt
        val trainEndDt = trainStartDt.plusDays(trainDayCount)
        val trainStartDtStr = trainStartDt.toString("yyyy-MM-dd")
        val trainEndDtStr = trainEndDt.toString("yyyy-MM-dd")
        val testEndDt = maxDt
        val testStartDt = testEndDt.minusDays(testDayCount)
        val testStartDtStr = testStartDt.toString("yyyy-MM-dd")
        val testEndDtStr = testEndDt.toString("yyyy-MM-dd")
        val wsfEndDt = trainEndDt
        val wsfStartDt = wsfEndDt.minusDays(wsfweightDays)
        val wsfEndDtStr = wsfEndDt.toString("yyyy-MM-dd")
        val wsfStartDtStr = wsfStartDt.toString("yyyy-MM-dd")
        (htl, (wsfStartDtStr, wsfEndDtStr, trainStartDtStr, trainEndDtStr, testStartDtStr, testEndDtStr))
    }

  }

  private def readFileLines(path: String) = {
    val file = new File(path)
    val pathList = file.listFiles().map(_.getAbsolutePath)
    pathList.flatMap {
      p =>
        Source.fromFile(p).getLines()
    }.toList
  }

  private def srcColIdx(colName: String) = colName match {
    case "htl_cd" => 0
    case "seg_cd" => 1
    case "is_member" => 2
    case "live_dt" => 3
    case "adv_bk_days" => 4
    case "rns" => 5
    case "rev" => 6
  }
}
