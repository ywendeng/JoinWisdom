package cn.jw.rms.version.creation

import com.typesafe.config.Config


case class KV(name: String, value: String)

object KV {
  def apply(params: Config): KV = {
    KV(params.getString("name"), params.getString("value"))
  }
}
