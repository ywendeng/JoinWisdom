package cn.jw.rms.data.framework.common.rules

import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

trait CleanerAssembly {
  /**
    * Clean data
    * @param sc SparkContext object on framework
    * @param config Config object, can get assembly parameters by config
    * @param prevStepRDD Data set RDD[String] of previous job
    * @return Data set RDD[String] of current job
    */
  def clean(sc: SparkContext, config: Config,
            prevStepRDD: Option[RDD[String]] = None): Option[RDD[String]]

  /***
    * If job succeed
    * @return (Boolean, String), 1st: will be true or false, true means succeed, false means failed; 2nd: will be "" if 1st is true, will be error message if 1st is false
    */
  def succeed: (Boolean, String)
}
