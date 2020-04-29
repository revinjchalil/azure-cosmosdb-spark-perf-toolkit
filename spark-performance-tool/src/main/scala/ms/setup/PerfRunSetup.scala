package ms.setup

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import ms.util.logs.LogUtils

/**
 * Sets up log directory and write out cluster conf
 * @param logUtils
 */
class PerfRunSetup(logUtils: LogUtils) {
  def setup(): Unit = {
    logUtils.setupOutputDir()
    logUtils.writeConf()
  }

}

object PerfRunSetup {
  def apply(logUtils: LogUtils): PerfRunSetup = new PerfRunSetup(logUtils)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf(true)
    conf.setAppName("PerfRunSetup")
    val sparkSession =
      SparkSession.builder.enableHiveSupport().config(conf).getOrCreate()

    val outputPath = args(0)
    val tagName = args(1)
    val scaleFactor = args(2)
    val storageSecretKey = if (args.length == 4) args(3) else null

    PerfRunSetup(LogUtils(outputPath, tagName, scaleFactor, storageSecretKey, sparkSession)).setup()

  }
}
