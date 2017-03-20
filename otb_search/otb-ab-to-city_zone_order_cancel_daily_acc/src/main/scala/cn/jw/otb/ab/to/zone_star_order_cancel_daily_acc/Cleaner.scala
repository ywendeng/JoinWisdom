package cn.jw.otb.ab.to.zone_star_order_cancel_daily_acc

import java.io.Serializable

import cn.jw.rms.data.framework.common.rules.CleanerAssembly
import cn.jw.rms.data.framework.common.utils.HDFSUtil
import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.io.Path
import scala.util.{Failure, Success, Try}



class Cleaner extends CleanerAssembly with Serializable {


  var gotSucceed = false
  var errMsg = ""

  override def clean(sc: SparkContext, config: Config, prevStepRDD: Option[RDD[String]]): Option[RDD[String]] = {
    Try {
      val hdfsHost = config.getString("hdfsHost")
      val saveToFile = config.getBoolean("save-result-to-file")
      val fieldSeparator = config.getString("field-splitter")
      val RDD_repartition_num = config.getInt("RDD-repartition-num")
      val cancelDailyOrderDir = config.getString("cancelDaily-order-dir")
      val dt = config.getString("dt")
      val testOpen = config.getBoolean("test-open")
      val cancelDailyPath = if (testOpen) cancelDailyOrderDir else addDtPath(cancelDailyOrderDir, dt)
      val outputDir = config.getString("dist-dir")

      val cancelDailyOrderOriginal = sc.textFile(cancelDailyPath).map(_.split(fieldSeparator))

      val cancelDailyOrderData = cancelDailyOrderOriginal.map {
        line =>
          val starId = line(colIndexAdvCcDay("starId"))
          val zoneId = line(colIndexAdvCcDay("zoneId"))
          val liveDt = line(colIndexAdvCcDay("liveDt"))
          val rns = line(colIndexAdvCcDay("rns")).toFloat
          val rev = line(colIndexAdvCcDay("rev")).toFloat
          val advCcDays = line(colIndexAdvCcDay("advCcDays")).toInt
          ((zoneId, starId, liveDt), (advCcDays,rns, rev))
      }

     val initialResult = cancelDailyOrderData.groupByKey.flatMap{
       case(key,value)=>
         val starId = key._2
         val zoneId = key._1
         val liveDt = key._3
         val sortAdvCcDays = value.toArray.sortBy {case(advCcDays,rns, rev)=> advCcDays }.reverse
         val scanSum = sortAdvCcDays.scanLeft((0,0f,0f)){
           (a,b)=>
           val total = (b._1,a._2 + b._2 ,a._3 + b._3)
             total
         }.tail.map{
           lines=>
             val advBkDay = lines._1
             val rns = lines._2
             val rev = lines._3
             val remark = ""
             s"$zoneId#$starId#$remark#$liveDt#$advBkDay#$rns#$rev"
         }
        scanSum
     }
      val result = if (!RDD_repartition_num.equals(1)) {
        initialResult.repartition(RDD_repartition_num)
      } else initialResult
      val outputPath = if (testOpen) outputDir else addDtPath(outputDir, dt)
      if (!hdfsHost.equals("local")) {
        HDFSUtil.delete(hdfsHost, outputPath)
      } else {
        val out_dir = Path(outputPath)
        if (out_dir.exists) out_dir.deleteRecursively()
      }

      if (saveToFile) {
        result.saveAsTextFile(outputPath)
      }

      result

    } match {
      case Success(res) =>
        gotSucceed = true
        Some(res)
      case Failure(e) =>
        gotSucceed = false
        errMsg = e.getMessage
        e.printStackTrace()
        None
    }

  }

  private def addDtPath(path: String, dt: String): String = {
    if (path.endsWith("/")) {
      path + dt
    } else {
      path + "/" + dt
    }
  }



  private def colIndexAdvCcDay(colname: String): Int = colname match {
    case "zoneId" => 0
    case "starId" => 1
    case "remark" => 2
    case "liveDt" => 3
    case "advCcDays" => 4
    case "rns" => 5
    case "rev" => 6
  }

  override def succeed: (Boolean, String) = (gotSucceed, errMsg)
}