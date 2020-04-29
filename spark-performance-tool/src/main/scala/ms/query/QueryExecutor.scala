package ms.query

import java.time.Instant
import java.util.Locale

import scala.io.Source
import net.liftweb.json._
import net.liftweb.json.Serialization.write
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SaveMode, SparkSession}
import ms.common.{QueryMetrics, RunTimes}
import ms.common.PerfSuiteType
import ms.config
import ms.util.FSUtil
import ms.util.PerfSuiteOptionsParser
import ms.util.logs.LogUtils
import com.microsoft.azure.cosmosdb.spark.schema._
import com.microsoft.azure.cosmosdb.spark._
import com.microsoft.azure.cosmosdb.spark.config.Config

/**
 * QueryExecutor class to run the TPCDS sql queries, records the metrics and writes to
 * the outputMetricsPath in json format.
 *
 * @param sparkSession spark session.
 * @param databaseName database name, on which the queries will be executed.
 * @param queryNames    query names, comma separated.
 * @param outputPath   path where the output metrics will be written in json format.
 * @param runsPerQuery number of times each TPCDS query should be run.
 */
abstract class QueryExecutor(sparkSession: SparkSession,
                             databaseName: String,
                             queryNames: String,
                             outputPath: String,
                             runsPerQuery: Int,
                             fsUtil: FSUtil) extends Logging {

  def queryBasePath: String

  def queryResultValidator: QueryResultValidator

  def execute(): List[QueryMetrics] = {

    sparkSession.sql(s"use $databaseName")
    var metrics = List[QueryMetrics]()
    queryNames.split(config.queryNameSeparator).foreach { qName =>
      metrics = metrics ::: executeSingleQuery(qName.trim)
    }
    metrics
  }

  private def executeSingleQuery(qName: String): List[QueryMetrics] = {
    var metrics = List[QueryMetrics]()
    try {
      logInfo(s"Executing query $qName")
      val queryPath = s"$queryBasePath/$qName.sql"
      val queryString = getQuery(queryPath)
      1 to runsPerQuery foreach { runId =>
        var startTime: Long = -1
        var endTime: Long = -1
        var executionDuration: Long = -1
        var queryResultValidationStatus: Boolean = false

        logInfo(s"The query from file named $qName is $queryString")

        try {
          startTime = Instant.now().toEpochMilli()
          val df = sparkSession.sql(queryString).collect()
          endTime = Instant.now().toEpochMilli()
          executionDuration = (endTime - startTime) // in milliseconds
          queryResultValidationStatus = queryResultValidator.validate(qName, df)
        } catch {
          case exception: Exception => logError(s"Execution failed for query: $qName." +
            exception.printStackTrace())
        } finally {
          logInfo(
            s"For $qName, startTime = $startTime, executionDuration = $executionDuration")

          val queryMetrics = QueryMetrics(
            qName,
            runId,
            RunTimes(startTime, endTime, executionDuration),
            queryResultValidationStatus
          )
          metrics = queryMetrics :: metrics
        }
      }
      metrics
    } finally {
      implicit val formats = DefaultFormats
      val outputMetricsJsonString = write(metrics)
      val outputmetricsPath = s"$outputPath/query-metrics/queryId=$qName/metricsJson"
      // scalastyle:off println
      println(outputMetricsJsonString)
      // scalastyle:on println
      fsUtil.writeAsFile(outputmetricsPath, outputMetricsJsonString)
    }
  }

  private def getQuery(pathToResources: String) : String = {
    Source.fromInputStream(getClass.getClassLoader.getResourceAsStream(pathToResources)
    ).getLines.mkString(" ")
  }

}

object QueryExecutor {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf(true)
    conf.setAppName("QueryExecution")
    val sparkSession =
      SparkSession.builder.enableHiveSupport().config(conf).getOrCreate()


    val optionParser = PerfSuiteOptionsParser()
    val argValues = optionParser.parseOptions(args)
    val queriesToSkip = argValues.queriesToSkip.split(config.queryNameSeparator).toSet
    val queriesToRun = argValues.queriesToRun.get.split(
      config.queryNameSeparator).filterNot(queriesToSkip).mkString(",")
    val storageSecretKey = argValues.storageSecretKey
    val metricsOutputPath = argValues.metricsOutputPath
    val tagName = argValues.tagName
    val scaleFactor = argValues.scaleFactor
    val fsUtil = Option(storageSecretKey).map(FSUtil(metricsOutputPath, _)).getOrElse(
      FSUtil(metricsOutputPath))
    val logUtils = new LogUtils(fsUtil, metricsOutputPath, tagName, scaleFactor, sparkSession)
    val preQueryProcessor = PreQueryProcessor(sparkSession,
      argValues.systemMetricsPort)
    val postQueryProcessor = PostQueryProcessor(sparkSession,
      logUtils,
      argValues.systemMetricsPort,
      queriesToRun,
      argValues.storageSecretKey)

//    val date_dim = sparkSession.sql("select * from tpcds1.date_dim")
//    val date_dimConfig = Map("Endpoint" -> "https://olap-sqlod.documents.azure.com:443/"
//      ,"Masterkey" -> "9hRai9e184CWS4NxL2mmDyro3pJBPP6NzNIz1voFfjVu6WXWoz8S37HFxe3DaApkXeybuRsk7ZDpe7Bu9VYpPA=="
//      ,"Database" -> "tpcds"
//      ,"Collection" -> "date_dim")
////    df_date_dim.write.mode(saveMode = "Overwrite").cosmosDB(customerConfig)
//    date_dim.write.format("com.microsoft.azure.cosmosdb.spark").options(date_dimConfig).mode(SaveMode.Overwrite).save()

    preQueryProcessor.execute()
    QueryExecutor(sparkSession,
      argValues.databaseName,
      argValues.scaleFactor,
      queriesToRun,
      argValues.runsPerQuery.toInt,
      argValues.storageSecretKey,
      argValues.tagName,
      argValues.perfSuiteType,
      logUtils,
      fsUtil).execute()
    postQueryProcessor.execute()

  }

  def apply(sparkSession: SparkSession,
            databaseName: String,
            scaleFactor: String,
            queryNames: String,
            runsPerQuery: Int,
            storageSecretKey: String,
            tagName: String,
            perfSuiteType: String,
            logUtils: LogUtils,
            fsUtil: FSUtil): QueryExecutor = {

    QueryExecutorFactory().get(sparkSession,
      databaseName,
      scaleFactor,
      queryNames,
      logUtils.getOutputPath(),
      runsPerQuery,
      fsUtil,
      PerfSuiteType.getPerfSuiteType(perfSuiteType.toUpperCase(Locale.ROOT)))

  }
}
