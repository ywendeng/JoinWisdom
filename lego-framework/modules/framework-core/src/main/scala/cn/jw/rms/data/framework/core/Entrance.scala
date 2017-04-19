package cn.jw.rms.data.framework.core

import cn.jw.rms.data.framework.common.utils.{HttpClient, TimerMeter}
import cn.jw.rms.data.framework.core.config.{AssemblyConf, FrameworkConfig}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

import collection.JavaConversions._
import scala.util.control.Breaks
import scala.util.{Failure, Success, Try}
import cn.jw.rms.data.framework.common._


object Entrance {

  val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val configPath =
      if(args.length == 1)
        args(0)
      else {
        logger.warn("No specify configuration file path. Using default configuration file path")
        "conf/application.conf"
      }

    val conf = FrameworkConfig.load(configPath)
    val frmwkName = conf.getString("name")
    val assembliesDir = conf.getString("assemblies-dir")
    val assemblies = conf.getConfigList("assemblies").map(AssemblyConf.apply).filter(_.enable).sortBy(_.index)
    val params = conf.getConfigList("parameters")
    val mailAPIUrl = conf.getString("mail.api.url")
    val mailTo = conf.getStringList("mail.to")
    //val mailsender = new MailSender(mailHost, mailPort, mailAuth, mailUser, mailPass)

    val sparkConf = new SparkConf().setAppName(frmwkName)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("io.compression.codecs", "com.hadoop.compression.lzo.LzopCodec")
    val sc = new SparkContext(sparkConf)

    var result: Option[RDD[String]] = None
    var currJob = ""
    val allJobs = assemblies.map(_.name).mkString(",")
    val start = TimerMeter.start("All job")
    var allSucceed = true
    Try {
      val mybreaks = new Breaks
      import mybreaks.{break, breakable}
      breakable {
        for (idx <- assemblies.indices) {
          val a = assemblies(idx)
          currJob = a.name
          val assemblyStart = TimerMeter.start(a.name)
          val succeedInfo = if (a.aType == "cleaner") {
            val loadStart = TimerMeter.start("load " + a.name)
            val cleaner = AssemblyLoader.loadCleaner(assembliesDir + a.jarName, a.className)
            TimerMeter.end("load " + a.name, loadStart)
            val c = params.filter(_.getString("name") == a.name).head
            result = cleaner.clean(sc, c, result)
            cleaner.succeed
          } else /*if (a.aType == "model")*/ {
            val loadStart = TimerMeter.start("load " + a.name)
            val model = AssemblyLoader.loadModel(assembliesDir + a.jarName, a.className)
            TimerMeter.end("load " + a.name, loadStart)
            val c = params.filter(_.getString("name") == a.name).head
            result = model.predict(sc, c, result)
            model.succeed
          }

          if (succeedInfo._1) {
            val msg = "%s Job [%s] execute succeed. %s".format(MY_LOG_PRE, a.name, succeedInfo._2)
            logger.info(msg)
            println(msg)
          } else {
            allSucceed = false
            val msg = "%s Job [%s] execute failed, error is [%s]".format(MY_LOG_PRE, a.name, succeedInfo._2)
            logger.error(msg)
            println(msg)
            val subject = s"[RMS][WARN!!] Spark process model [$frmwkName] failed"
            val err = succeedInfo._2
            val body = s"Process all Jobs [$allJobs] failed, failed job is [$currJob], error is [$err]"
            val mailResp = sendMail(mailAPIUrl, subject, body, mailTo)
            logger.info(s"The resp of sending mail [$subject] is [$mailResp]")
            break()
          }
          TimerMeter.end(a.name, assemblyStart)
        }
      }
    } match {
      case Success(v) =>
        val msg = "%s All of job succeed.".format(MY_LOG_PRE)
        logger.info(msg)
        println(msg)
        val ms = TimerMeter.end("All Job", start)
        if (allSucceed) {
          val subject = s"[RMS] Spark process model [$frmwkName] succeed"
          val body = s"Process all Jobs [$allJobs] succeed, Estimated Time = $ms ms"
          val mailResp = sendMail(mailAPIUrl, subject, body, mailTo)
          logger.info(s"The resp of sending mail [$subject] is [$mailResp]")
        }
      case Failure(e) =>
        val err = e.toString
        val msg = s"$MY_LOG_PRE Job failed, $err"
        logger.error(msg)
        println(msg)
        e.printStackTrace()
        val subject = s"[RMS][WARN!!] Spark process model [$frmwkName] failed"
        val body = s"Process all Jobs [$allJobs] failed, failed job is [$currJob], exception is [$err]"
        val mailResp = sendMail(mailAPIUrl, subject, body, mailTo).body
        logger.info(s"The resp of sending mail [$subject] is [$mailResp]")
    }
  }

  private def sendMail(url: String, subject: String, body: String, mailTo: java.util.List[String]) = {
    val jsonTpl =
        """
          |{
          | "email_receiver":"%s",
          | "email_subject":"%s",
          | "email_content":"%s"
          |}
        """.stripMargin
    val mailToStr = mailTo.mkString(",")
    val json = jsonTpl.format(mailToStr, subject, body)
    HttpClient.postJson(url, json)

  }
}


//spark-submit --class cn.jw.rms.data.framework.core.Entrance --master local[4] --driver-memory 6G /Users/deanzhang/work/code/pms/rms-data-framework/framework-core-assembly-1.0.jar