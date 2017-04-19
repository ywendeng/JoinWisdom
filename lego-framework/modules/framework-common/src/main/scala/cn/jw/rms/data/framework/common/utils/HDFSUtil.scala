package cn.jw.rms.data.framework.common.utils


object HDFSUtil {

  def delete(hdfsHost: String, output: String, isRecusrive: Boolean = true) = {
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(hdfsHost), hadoopConf)
    hdfs.delete(new org.apache.hadoop.fs.Path(output), isRecusrive)
  }

}
