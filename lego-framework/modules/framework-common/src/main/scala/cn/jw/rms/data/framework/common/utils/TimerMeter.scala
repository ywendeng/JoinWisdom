package cn.jw.rms.data.framework.common.utils

import org.slf4j.LoggerFactory
import cn.jw.rms.data.framework.common._

/**
  * Created by deanzhang on 15/12/31.
  */
object TimerMeter {
  val logger = LoggerFactory.getLogger(this.getClass)

  def start(jobName: String) = {
    val ns = System.nanoTime()
    logger.info(s"$MY_LOG_PRE Start [$jobName] ns = $ns")
    ns
  }

  def end(jobName: String, start: Long) = {
    val ns = System.nanoTime()
    val ms = (ns - start) / 1E6
    logger.info(s"$MY_LOG_PRE End [$jobName] ns = $ns, Elapsed Time = " + ms + "ms")
    ms
  }

}
