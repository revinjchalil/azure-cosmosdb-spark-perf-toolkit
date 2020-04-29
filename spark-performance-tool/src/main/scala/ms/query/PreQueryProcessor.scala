package ms.query

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

import ms.util.RestClient

/**
 * PreQueryProcessor-> takes some actions before query execution starts
 * @param sparkSession
 * @param systemMetricsPort
 * @param systemMetricsClient
 */
class PreQueryProcessor(sparkSession: SparkSession,
                        systemMetricsPort: Option[Int],
                        systemMetricsClient: RestClient) extends Logging {
  def startSystemMetricsCollection(): Unit = {
    val executors = sparkSession.sparkContext.getExecutorMemoryStatus.map(_._1).toList
    val executorHostSet = executors.map(x => x.split(":")(0)).toSet
    logInfo(s"List of hosts : $executorHostSet")
    executorHostSet.foreach(host => {
      val url = "http://" + host + ":" + systemMetricsPort.get + "/collectd/start"
      try{
        logInfo(s"making a post request to url : $url to start collectd agent on $host")
        systemMetricsClient.postRequest(url)
        logInfo(s"Sucessfully started collectd on host: $host")
      } catch {
        case exception: Exception =>
          logWarning(s"Failed to start collectd on $host, exception: ${exception.getMessage}")
      }
    })
  }

  def execute(): Unit = {
    logInfo(s"systemMetrics port is : $systemMetricsPort")
    if (systemMetricsPort.isDefined) {
      startSystemMetricsCollection()
    }
  }
}

object PreQueryProcessor {
  def apply(sparkSession: SparkSession, systemMetricsPort: Option[Int]): PreQueryProcessor =
    new PreQueryProcessor(sparkSession, systemMetricsPort, new RestClient)
}
