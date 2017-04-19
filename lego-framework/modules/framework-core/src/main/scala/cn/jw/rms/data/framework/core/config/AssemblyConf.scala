package cn.jw.rms.data.framework.core.config

import com.typesafe.config.Config


case class AssemblyConf(name: String,
                        index: Int,
                        aType: String,
                        jarName: String,
                        className: String,
                        enable: Boolean)

object AssemblyConf{
  def apply(params: Config): AssemblyConf = {
    AssemblyConf(params.getString("name"),
      params.getInt("index"),
      params.getString("type"),
      params.getString("jar-name"),
      params.getString("class-name"),
      params.getBoolean("enable")
    )
  }
}
