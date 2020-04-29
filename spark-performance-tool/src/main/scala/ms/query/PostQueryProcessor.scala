package ms.query

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

import ms.config
import ms.util.RestClient
import ms.util.logs.LogUtils

/**
 * PostQueryProcessor-> after the execution of the queries, PostQueryProcesor uploads collectd
 * system metrics for each of the executor and driver hosts. It does so by making post requests
 * to the hosts.
 * @param sparkSession
 * @param logUtils
 * @param systemMetricsPort
 * @param queriesToRun
 * @param secretKey
 * @param systemMetricsClient
 */
class PostQueryProcessor(sparkSession: SparkSession,
                         logUtils: LogUtils,
                         systemMetricsPort: Option[Int],
                         queriesToRun: String,
                         secretKey: String,
                         systemMetricsClient: RestClient) extends Logging {

  private val localCollectdFilePath = "/var/lib/collectd/csv"
  /**
   * stops system metrics collection on each host and uploads the host metrics
   * to systemMetricsOutputDir
   */
  def stopSystemMetricsCollection(): Unit = {
    val outputPath = logUtils.getOutputPath()
    val executors = sparkSession.sparkContext.getExecutorMemoryStatus.map(_._1).toList
    val executorHostSet = executors.map(x => x.split(":")(0)).toSet
    var systemMetricsOutputDir = ""
    if(queriesToRun.split(config.queryNameSeparator).size == 1) {
      systemMetricsOutputDir = s"$outputPath/system-metrics/queryId=$queriesToRun"
    } else {
      systemMetricsOutputDir = s"$outputPath/system-metrics/queryId=all"
    }
    val setOfFutures = executorHostSet.map(host => Future {
      val url = "http://" + host + ":" + systemMetricsPort.get + "/collectd/stop"
      val postData =
        s"""{"fsuri" : "$systemMetricsOutputDir/host=$host", "secretKey" : "$secretKey"}"""
      logInfo(s"Making a post request to url $url with data: $postData to stop the " +
        s"collectd agent and upload the system metrics data")
      try {
        systemMetricsClient.postRequest(url, postData)
      } catch {
        case e: Exception =>
          logWarning(s"Failed to upload system metrics data for host: $host")
          throw new Exception(s"Failed to upload system metrics from " +
            s"$localCollectdFilePath/$host to " +
            s"$systemMetricsOutputDir/host=$host, exception cause: " +
            s"${e.getMessage}")
      }
      logInfo(s"Post request to $url successful")
      s"Uploaded system metrics data from $localCollectdFilePath/$host" +
        s" to $systemMetricsOutputDir/host=$host"
    })

    // converts a Future[T] to Future[Try[T]]
    def futureToFutureTry[T](f: Future[T]): Future[Try[T]] =
      f.map(Success(_)).recover({case x => Failure(x)})

    val setOfFutureTrys = setOfFutures.map(futureToFutureTry)
    // scalastyle:off awaitresult
    val futureSetOfTrys = Future.sequence(setOfFutureTrys)
    val resultOfFuture = Await.result(futureSetOfTrys, 5 minutes)
    // scalastyle:on awaitresult
    resultOfFuture.foreach {
      case Failure(exception) => logWarning(s"${exception.getMessage}")
      case Success(futureResult) => logInfo(s"$futureResult")
    }
  }

  def execute(): Unit = {
    logInfo("spark-events backup starting.")
    logUtils.backup(queriesToRun)
    logInfo("spark-events backup completed.")
    logInfo(s"systemMetrics port is : $systemMetricsPort")
    if (systemMetricsPort.isDefined) {
      stopSystemMetricsCollection()
    }
  }
}

object PostQueryProcessor {
  def apply(sparkSession: SparkSession,
            logUtils: LogUtils,
            systemMetricsPort: Option[Int],
            queriesToRun: String,
            secretKey: String): PostQueryProcessor =
    new PostQueryProcessor(sparkSession, logUtils, systemMetricsPort,
      queriesToRun, secretKey, new RestClient)
}
