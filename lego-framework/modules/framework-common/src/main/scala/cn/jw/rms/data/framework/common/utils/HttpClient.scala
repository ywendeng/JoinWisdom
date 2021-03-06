package cn.jw.rms.data.framework.common.utils

import scalaj.http.Http


object HttpClient {

  def postJson(url: String, json: String) = {
    Http(url).postData(json).header("content-type", "application/json").asString
  }

  /*def main(args: Array[String]): Unit ={
    val url = "http://42.62.78.140:8002/WarningEmail/"
    val json =
      """
        |{
        | "email_receiver":"xnzhang@jointwisdom.cn",
        | "email_subject":"test warning email API1",
        | "email_content":"test warning email API2"
        |}
      """.stripMargin
    val resp = HttpClient.postJson(url, json)
    println("resp = " + resp.body)
  }*/

}
