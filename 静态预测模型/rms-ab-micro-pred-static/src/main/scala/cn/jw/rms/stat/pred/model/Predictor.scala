package cn.jw.rms.stat.pred.model


import java.io.Serializable


import cn.jw.rms.data.framework.common.rules.PredictModelAssembly
import cn.jw.rms.data.framework.common.utils.HDFSUtil
import com.typesafe.config.Config
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.joda.time.{DateTime, Days}

import scala.util.{Failure, Success, Try}
import scala.reflect.io.Path



object SeriLogger extends Serializable {
  @transient lazy val logger = Logger.getLogger(getClass.getName)
}
/**
  * Created with IntelliJ IDEA.
  * Author:DengYuanWen
  * Date:2016/7/24
  * Time:13:21
  * Email:dengyw@jointwisdom.cn
  */
class Predictor extends PredictModelAssembly with Serializable {
  val MAT_COL_LEN = 33
  var gotSucceed = false
  var errMsg = ""

  override def accuracy(sc: SparkContext, config: Config, prevStepRDD: Option[RDD[String]]): String = "Not Supported"

  override def succeed: (Boolean, String) = (gotSucceed, errMsg)

  /**
    * @param sc          SparkContext实例对象，用于从获取文件对应的RDD数据集
    * @param config      Config 对象实例，主要用于从配置文件中读取配置文件信息
    * @param prevStepRDD 前置依赖数据集
    * @return 处理结果数据集
    */
  override def predict(sc: SparkContext, config: Config, prevStepRDD: Option[RDD[String]]): Option[RDD[String]] = {
    Try {
      val checkpointDir = config.getString("checkpoint-dir")
      sc.setCheckpointDir(checkpointDir)

      val rmsActualHistoryDir = config.getString("htl-actual-history-dir")

      val regex = config.getString("field-splitter")
      val distDir = config.getString("dist-dir")

      val RDDReparNum = config.getInt("RDD-repartition-num")
      val checkpointNum = config.getInt("checkpoint-num")
      val saveToFile = config.getBoolean("save-result-to-file")
      val hadoopHost = config.getString("hadoop-host")
      val histEable = config.getBoolean("hist-enable")
      val fcDateStr = config.getString("fc-date")
      val fcDays = config.getInt("fc-days")
      val rmsPredDataTagDir = config.getString("rms-pred-dateTag-dir")
      val pred_target=config.getString("pred-target")
      val histStartStr = config.getString("hist-start")
      val histEndStr = config.getString("hist-end")
      val defaultHistDays = config.getInt("default-hist-days")

      /**
        * 根据预测目标设置预测类型参数
        */
      val fc_typ: Int =if(pred_target=="arrival"){
        1
      }else if(pred_target=="leave"){
        2
      }else if(pred_target=="no_show"){
        3
      }else if(pred_target=="cancel"){
        4
      }else
      //此处为备用
        5




      /**
        * 获取预测tag标签数据集
        */
      val dateTagRaw = sc.textFile(rmsPredDataTagDir).map(_.split("#", -1)).filter(_.nonEmpty)
        .map {
          line =>
            val htlCd = line(collumeIndexDateTag("htl_cd"))
            val statDt = line(collumeIndexDateTag("stat_dt"))
            val seasonTag: String = line(collumeIndexDateTag("season_tag"))
            val specialTag: Array[String] = line(collumeIndexDateTag("special_tag")).split(",",-1)
            if(specialTag(0)==""){
            ((htlCd, statDt), Array(seasonTag))
            } else
              ((htlCd, statDt), specialTag)
        }

      /**
        * 设置预测日期，如果为"now"表示需要预测当天的，否则切分开始日期和结束日期
        */
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

      /**
        *根据配置文件中的histEable来确定是否需要过去历史时间对应的数据集来生成预测值
        */
      val  rmsHotelCdStatDtTag =if(histEable){
        val histStartDt = new DateTime(histStartStr)
        val histEndDt = new DateTime(histEndStr)
        val histStartDtNum = histStartDt.toString("yyyyMMdd").toInt
        val histEndDtNum = histEndDt.toString("yyyyMMdd").toInt

        val rmsActualHist =  sc.textFile(rmsActualHistoryDir).map(_.split(regex, -1)).filter(_.nonEmpty)
            .filter { line =>
              val date = new DateTime(line(collumeIndexRmsActualHist("stat_dt"))).toString("yyyyMMdd").toInt
              histStartDtNum<=date && date<=histEndDtNum
            }
        /**
          * 得到酒店ID和相应tag对应的值
          */
        val res = getRmsHotelCdStatTagValue(rmsActualHist,dateTagRaw,pred_target)
        Some(res)
      }else
        None



      /**
        * 开始进行时间预测
        */
      val startDt = fcDatePair._1
      val endDt = fcDatePair._2
      val countFcDate = Days.daysBetween(startDt, endDt).getDays



      val results = (0 to countFcDate).toArray.map {
        date =>
          /**
            * 根据历史的数据来预测
            */
          val currentFcDate = startDt.plusDays(date)
          val fcDateSimpStr = currentFcDate.toString("yyyyMMdd")
          val endDtNum =currentFcDate.minusDays(1).toString("yyyyMMdd").toInt;
          val startDtNum=currentFcDate.minusDays(defaultHistDays).toString("yyyyMMdd").toInt

          val dailyRmsHotelCdStatDtTag=if (histEable) {
            rmsHotelCdStatDtTag.get
          }else{
            //从HDFS中读取dt为当前的date的数据，并得到当前date的数据集
            val path = rmsActualHistoryDir + "/dt=" + fcDateSimpStr

            val dailyRmsActualHistRaw = if (RDDReparNum > 0)
              sc.textFile(path).filter(_.nonEmpty).map(_.split(regex, -1)).repartition(RDDReparNum)
            else
              sc.textFile(path).filter(_.nonEmpty).map(_.split(regex, -1))

            /**
              * 选出特定日期的数据记录
              */
            val dailyRmsActualHist=dailyRmsActualHistRaw.filter{
              line =>
                val date = new DateTime(line(collumeIndexRmsActualHist("stat_dt"))).toString("yyyyMMdd").toInt
               startDtNum<=date && date<=endDtNum
            }


            getRmsHotelCdStatTagValue(dailyRmsActualHist,dateTagRaw,pred_target)
          }

          val liveDtList: RDD[(String, String)] = sc.parallelize(1 to fcDays).map {
            day =>
              val liveDt = currentFcDate.plusDays(day).toString("yyyy-MM-dd")
              (liveDt, currentFcDate.toString("yyyy-MM-dd"))
          }
          val dateTag = dateTagRaw.map {
            case ((htlCd, statDt), (tagLst)) =>
              (statDt, (htlCd, tagLst))
          }

          val dateTagList= dateTag.join(liveDtList).flatMap{
          case (liveDt, ((htlCd, tagLst),fcDt)) =>

            tagLst.map{
                tag=>
                  ((htlCd, tag), (liveDt, fcDt,1))
              }
        }.join(dailyRmsHotelCdStatDtTag).map {
          case ((htlCd,tag), ((liveDt, fcDt,num), target)) =>
            ((htlCd,liveDt), (tag,fcDt,num, target))
        }
   /*    dateTagList.collect().foreach{
          case ((htlCd,liveDt), (tag,fcDt,num, target))=>
            println("result=>"+htlCd+"\t"+tag+"\t"+liveDt+"\t"+fcDt+"\t"+num+"\t"+target)
        }
   */
        val result= dateTagList.reduceByKey((a,b)=>{
            /**
              * 如果同一个live-dt同时属于不同的tag,则需要算平均值
              */
            val sum=a._3+b._3
            val avg=(a._4+b._4)/sum
            (b._1,b._2,sum,avg.toInt)
          }).map{
            case((htlCd,liveDt), (tag,fcDt,sum,avg))=>
            s"$htlCd#$fc_typ#$liveDt#$fcDt#$avg"
          }

          if (!histEable && saveToFile) {
            val distPath = distDir + s"/dt=$fcDateSimpStr"
            if (distDir.startsWith("hdfs://")) {
              HDFSUtil.delete(hadoopHost, distPath)
            } else {
              val path:Path = Path(distPath)
              path.deleteRecursively()
            }
            result.saveAsTextFile(distPath)
          }

        result
      }


      if (histEable && saveToFile) {
        val distPath = distDir
        if (distDir.startsWith("hdfs://")) {
          HDFSUtil.delete(hadoopHost, distPath)
        }  else {
          val path:Path= Path(distPath)
          path.deleteRecursively()
        }

        /**
          * 将Array[RDD[\]\]类型的数据合并为一个数据集RDD
          */
        if (results.nonEmpty) {
          var outputRDD = results(0)
          (1 until results.length).foreach {
            idx =>
              val currRDD = results(idx)
              outputRDD = outputRDD.cache().union(currRDD)
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

    } match {
      case Success(res) =>
        gotSucceed = true
        None
      case Failure(e) =>
        gotSucceed = false
        errMsg = e.toString
        println("error: " + e.toString)
        e.printStackTrace()
        None
    }
  }


  /**
    * 根据历史全量数据或者
    *
    * @param RmsActualHistRaw
    * @param dateTagRaw
    * @return
    */
  def getRmsHotelCdStatTagValue(RmsActualHistRaw: RDD[Array[String]],
                                dateTagRaw: RDD[((String, String), Array[String])],
                                pred_target: String)= {

    val  rmsActualHistValue= RmsActualHistRaw.map {
      line =>
        val htl_cd = line(collumeIndexRmsActualHist("htl_cd"))
        val stat_dt = line(collumeIndexRmsActualHist("stat_dt"))
        val target =  line(collumeIndexRmsActualHist(pred_target))



        ((htl_cd, stat_dt),target.toFloat.toInt )
      /**
        * 把具有同一个htl_cd 和stat_dt的数据求和
        */
    }.reduceByKey((a,b)=>a+b)

  val reduce = rmsActualHistValue.join(dateTagRaw).flatMap{
      case ((htl_cd, stat_dt), (target,dateTag)) =>
        dateTag.map(tag=>
          ((htl_cd, tag), (target,1))
        )
    }.reduceByKey((a,b)=>
    (a._1+b._1,a._2+b._2)
  )

   val reduceValue=reduce.map {
      case (key, value) =>
        (key, value._1/value._2)
    }
    /**
      *把相同htlCd 和tag数据记录求平均值
      */
    reduceValue
  }

  def collumeIndexSeason(s: String): Int = s match {
    case "htl_cd" => 0
    case "season" => 1
    case "start_dt" => 5
    case "end_dt" => 6
  }

  def collumeIndexRmsActualHist(s: String): Int = s match {
    case "htl_cd" => 0
    case "stat_dt" => 1
    case "arrival" => 6
    case "leave" => 7
    case "no_show" => 8
    case  "cancel"=> 10
  }

  def collumeIndexDateTag(s: String): Int = s match {
    case "htl_cd" => 0
    case "stat_dt" => 1
    case "season_tag" => 2
    case "special_tag" => 3
  }
}

