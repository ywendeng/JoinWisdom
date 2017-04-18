package cn.jw.rms.data.framework.core.config

import java.io.{FileReader, BufferedReader, File}
import java.nio.file.Files

import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory

import scala.io.Source

/**
  * Created by deanzhang on 15/11/29.
  */
object FrameworkConfig {

  val logger = LoggerFactory.getLogger(this.getClass)

  def load(path: String) = {

    /*for (line <- Source.fromFile(path).getLines()) {
      println(line)
    }*/

    val file = new File(path)

    //println("file.canRead = " + file.canRead)

    logger.info("config file path = " + file.getAbsolutePath)
    ConfigFactory.parseFile(file)
  }

}
