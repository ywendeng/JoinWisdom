package cn.jw.rms.pred.genconfig

import com.typesafe.config.Config

import scala.util.{Failure, Success, Try}


case class SimpleConf(name: String, srcDir: String, dt: Option[String] = None ,localDir: Option[String] = None,backupDir: Option[String] = None)

object SimpleConf {
  def apply(params: Config): SimpleConf = {
    val dtOpt = Try(params.getString("dt")) match {
      case Success(res) => Some(res)
      case Failure(e) => None
    }
    val localDir = Try(params.getString("local-dir")) match {
      case Success(res) => Some(res)
      case Failure(e) => None
    }
    val backupDir = Try(params.getString("backup-dir")) match {
      case Success(res) => Some(res)
      case Failure(e) => None
    }
    SimpleConf(
      params.getString("name"),
      params.getString("src-dir"),
      dtOpt,
      localDir,
      backupDir
    )
  }
}
