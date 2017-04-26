package cn.jw.rms.pred.genconfig

import com.typesafe.config.Config
import collection.JavaConversions._


case class ComplexConf(name: String, index:Int,srcList: Array[ComplexSrc])

object ComplexConf {
  def apply(params: Config): ComplexConf = {
    val srcList = params.getConfigList("src-list").map(ComplexSrc.apply).toArray
    ComplexConf(
      params.getString("name"),
      params.getInt("index"),
      srcList
    )
  }
}

case class ComplexSrc(name: String, dir: String, fieldList: Array[String])

object ComplexSrc {
  def apply(params: Config): ComplexSrc = {
    val fieldList = params.getStringList("field-list").toIndexedSeq.toArray
    ComplexSrc(
      params.getString("name"),
      params.getString("dir"),
      fieldList
    )
  }
}