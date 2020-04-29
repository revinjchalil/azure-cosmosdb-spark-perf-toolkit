package ms.util.logs

import scala.collection.immutable.ListMap
import scala.collection.mutable

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf

import ms.common.SparkConf
import ms.config
import ms.util.FSUtil

/**
 * Manage logs for example back up spark event logs etc.
 *
 * @param fsUtil       fs util for backing up the files.
 * @param sparkSession spark session.
 */
class LogUtils(fsUtil: FSUtil,
               outputLogPath: String,
               tag: String,
               scaleFactor: String,
               sparkSession: SparkSession)
  extends Logging {

  /**
   * create the output path for metrics and output logs
   */
  def setupOutputDir(): Unit = {
    val outputDirName = getOutputPath()
    logInfo(s"The output logs will be written in the location = $outputDirName")
    fsUtil.mkdir(outputDirName)
  }

  def getOutputPath(): String = {
    outputLogPath + s"/$tag/sf_$scaleFactor"
  }


  /**
   * Backup the logs at given output location.
   */
  def backup(queryName: String): Unit = {
    var outputDir = ""
    if(queryName.split(config.queryNameSeparator).size == 1) {
      outputDir = s"spark-events/queryId=$queryName"
    } else {
      outputDir = s"spark-events/queryId=all"
    }
    copySparkEvents(outputDir)
  }

  /**
   * copy spark event Logs for current app to the output path.
   */
  private def copySparkEvents(outputDirName: String): Unit = {
    val outputPath = getOutputPath() + s"/$outputDirName"
    val eventsFile = getSparkEventsFile()
    logInfo(s"Starting backup Spark Events from file: $eventsFile to the path $outputPath")
    fsUtil.copyDir(eventsFile.toString, s"$outputPath/${eventsFile.getName}")
  }

  private def getSparkEventsFile(): Path = {
    val sparkEventLogDir = sparkSession.conf.get("spark.eventLog.dir")
    val appId = sparkSession.sparkContext.applicationId
    val fsUtil = FSUtil(sparkEventLogDir)
    fsUtil.getFilePathList(sparkEventLogDir).filter(
      filePath => filePath.toString.contains(appId))(0)
  }

  /**
   * Write spark, sql and hadoop configs to output path in json format
   */
  def writeConf(): Unit = {
    val outputPath = getOutputPath()
    val mapper = new ObjectMapper().registerModule(DefaultScalaModule)

    val sparkConfSorted = getSparkConfSorted()
    val sqlConfSorted = getSparkSqlConfSorted()
    val hadoopConfSorted = getHadoopConfSorted()
    val conf = SparkConf(sparkConfSorted, sqlConfSorted, hadoopConfSorted)
    val confJson = mapper.writeValueAsString(conf)

    val outputConfPath = outputPath + "/confJson"
    // scalastyle:off println
    println(confJson)
    // scalastyle:on println
    fsUtil.writeAsFile(outputConfPath, confJson)
  }

  def getSparkConfSorted(): ListMap[Object, String] = {
    ListMap(sparkSession.sparkContext.getConf.getAll.toMap.toSeq.sortBy(_._1): _*)
  }

  def getSparkSqlConfSorted(): ListMap[Object, String] = {
    ListMap(SQLConf.get.getAllConfs.toSeq.sortBy(_._1): _*)
  }

  def getHadoopConfSorted(): ListMap[Object, String] = {
    val it = sparkSession.sparkContext.hadoopConfiguration.iterator
    val hadoopConf = mutable.Map[String, String]()
    while(it.hasNext) {
      val keyVal = it.next()
      hadoopConf.put(keyVal.getKey, keyVal.getValue)
    }
    ListMap(hadoopConf.toSeq.sortBy(_._1): _*)
  }

}

/**
 * Companion object for Log Manager.
 */
object LogUtils {
  def apply(outputLogPath: String, tag: String, scaleFactor: String, storageSecretKey: String,
            sparkSession: SparkSession): LogUtils = {
    val fsUtil =
      Option(storageSecretKey).map(FSUtil(outputLogPath, _)).getOrElse(FSUtil(outputLogPath))
    new LogUtils(fsUtil, outputLogPath, tag, scaleFactor, sparkSession)
  }
}
