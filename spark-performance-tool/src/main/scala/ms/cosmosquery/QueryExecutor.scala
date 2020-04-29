package ms.cosmosquery


import java.time.Instant
import java.util.{Calendar, Date, Locale}

import scala.io.Source
import net.liftweb.json._
import net.liftweb.json.Serialization.write
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import ms.common.{PerfSuiteType, QueryMetrics, QueryMetricsResults, RunTimes}
import ms.config
import ms.util.FSUtil
import ms.util.PerfSuiteOptionsParser
import ms.util.logs.LogUtils
import com.microsoft.azure.cosmosdb.spark.schema._
import com.microsoft.azure.cosmosdb.spark._
import com.microsoft.azure.cosmosdb.spark.config.Config
import ms.query.{PostQueryProcessor, PreQueryProcessor, QueryResultValidator}
import org.joda.time.{DateTime, Days}
import org.joda._


import scala.collection.mutable

/**
 * QueryExecutor class to run the TPCDS sql queries, records the metrics and writes to
 * the outputMetricsPath in json format.
 *
 * @param sparkSession spark session.
 * @param cosmosSynapseDatabaseName cosmos database name, on which the queries will be executed.
 * @param scaleFactor scale factor to generate QueryResultValidator path
 * @param queryNames query names, comma separated.
 * @param outputPath  path where the output metrics will be written in json format.
 * @param runsPerQuery number of times each TPCDS query should be run.
 */

class QueryExecutor( sparkSession: SparkSession,
                     skipCosmosOLTPQueryExecution: Boolean,
                     skipCosmosOLAPQueryExecution: Boolean,
                     cosmosEndpoint:String,
                     cosmosAccountKey:String,
                     cosmosRegion: String,
                     cosmosDatabaseName:String,
                     cosmosSynapseDatabaseName: String,
                     scaleFactor: String,
                     queryNames: String,
                     outputPath: String,
                     runsPerQuery: Int,
                     fsUtil: FSUtil) extends Logging {

  def queryBasePath: String = "tpcds/queries"
  def queryResultValidator: QueryResultValidator = QueryResultValidator(s"tpcds/validation/tpcds-${scaleFactor}")
  def collectionNamesPath: String = "tpcds/collections/names"

  def execute()  = {

    if (!skipCosmosOLTPQueryExecution) {
      sparkSession.sql(s"use ${cosmosSynapseDatabaseName}_OLTP")
      var metrics = List[QueryMetrics]()
      queryNames.split(config.queryNameSeparator).foreach { qName =>
        metrics = metrics ::: executeSingleQuery(qName.trim, "OLTP")
      }
    }

    if (!skipCosmosOLAPQueryExecution) {
      val cosmosTPCDSTables = getCollectionNames()
      createTempViews(cosmosTPCDSTables)

      var metrics = List[QueryMetrics]()
      queryNames.split(config.queryNameSeparator).foreach { qName =>
        metrics = metrics ::: executeSingleQuery(qName.trim, "OLAP")
      }

//      implicit val formats = DefaultFormats
//      val outputMetricsJsonString = write(metrics)
//      val outputmetricsPath = s"$outputPath/cosmos-query-metrics/queryType=OLAP/metricsJson"
      // scalastyle:off println
//      println(outputMetricsJsonString)
      // scalastyle:on println
//      fsUtil.writeAsFile(outputmetricsPath, outputMetricsJsonString)
    }
  }

  private def executeSingleQuery(qName: String, qType: String): List[QueryMetrics] = {
    var metrics = List[QueryMetrics]()
    try {
      logInfo(s"Executing query $qName against $qType store.")
      val queryPath = s"$queryBasePath/$qName.sql"
      val queryString = getQuery(queryPath)
      1 to runsPerQuery foreach { runId =>
        var startTime: Long = -1
        var endTime: Long = -1
        var startTimeLocal: String = null
        var endTimeLocal: String = null
        var executionDuration: Long = -1
        var queryResultValidationStatus: Boolean = false

        logInfo(s"The query from file named $qName is $queryString")

        try {
          startTime = Instant.now().toEpochMilli()
          startTimeLocal = DateTime.now().toString()
          val df = sparkSession.sql(queryString).collect()
          endTime = Instant.now().toEpochMilli()
          endTimeLocal = DateTime.now().toString()
          executionDuration = (endTime - startTime) // in milliseconds
          queryResultValidationStatus = queryResultValidator.validate(qName, df)
        } catch {
          case exception: Exception => logError(s"Execution failed for query: $qName against $qType store." +
            exception.printStackTrace())
        } finally {
          logInfo(
            s"For $qName against $qType store, startTime = $startTime, executionDuration = $executionDuration")

          val queryMetrics = QueryMetrics(
            qName,
            runId,
            RunTimes(startTime, endTime, executionDuration),
            queryResultValidationStatus
          )
          metrics = queryMetrics :: metrics

          import sparkSession.implicits._

          val queryResults = QueryMetricsResults(qType, qName, runId, startTimeLocal, endTimeLocal, queryResultValidationStatus, executionDuration, executionDuration / 60000)
          val queryResultsList = mutable.MutableList[QueryMetricsResults]()
          queryResultsList += queryResults

          val queryResultsListDF = queryResultsList.toSeq.toDF()
          queryResultsListDF.createOrReplaceTempView("queryResultsListDF")

          sparkSession.sql("insert into tpcds_metrics.cosmos_query_metrics_v2 select * from queryResultsListDF")

        }
      }
      metrics
    }
    finally {
      implicit val formats = DefaultFormats
      val outputMetricsJsonString = write(metrics)
//      val outputmetricsPath = s"$outputPath/cosmos-query-metrics/queryType=$qType/queryId=$qName/metricsJson"
      // scalastyle:off println
      println(outputMetricsJsonString)
      // scalastyle:on println
//      fsUtil.writeAsFile(outputmetricsPath, outputMetricsJsonString)
    }
  }

  private def getCollectionNames() : Array[String] = {
    val bufferedSource = Source.fromInputStream(getClass.getClassLoader.getResourceAsStream(collectionNamesPath))
    val lines = (for (line <- bufferedSource.getLines()) yield line).toArray
    bufferedSource.close
    lines
  }

  private def createTempViews(cosmosTPCDSTables: Array[String]) : Unit = {
    cosmosTPCDSTables.foreach { table => {
      val config = Map(
                      "spark.cosmos.accountendpoint" -> s"${cosmosEndpoint}",
                      "spark.cosmos.accountkey" -> s"${cosmosAccountKey}",
                      "spark.cosmos.region" -> s"${cosmosRegion}",
                      "spark.cosmos.database" -> s"${cosmosDatabaseName}",
                      "spark.cosmos.container" -> s"""${table}"""
                      )
      val df = sparkSession.read.format("cosmos.olap").options(config).load()
      df.createOrReplaceTempView(s"""${table}""")
      }
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
    conf.setAppName("CosmosQueryExecution")
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

    QueryExecutor(sparkSession,
      argValues.skipCosmosOLTPQueryExecution,
      argValues.skipCosmosOLAPQueryExecution,
      argValues.cosmosEndpoint,
      argValues.cosmosAccountKey,
      argValues.cosmosRegion,
      argValues.cosmosDatabaseName,
      argValues.cosmosSynapseDatabaseName,
      argValues.scaleFactor,
      queriesToRun,
      argValues.runsPerQuery.toInt,
//      argValues.storageSecretKey,
//      argValues.tagName,
      argValues.perfSuiteType,
      logUtils,
      fsUtil).execute()
  }

  def apply(sparkSession: SparkSession, skipCosmosOLTPQueryExecution: Boolean, skipCosmosOLAPQueryExecution: Boolean,  cosmosEndpoint: String, cosmosAccountKey:String, cosmosRegion: String, cosmosDatabaseName: String, cosmosSynapseDatabaseName: String, scaleFactor: String, queriesToRun: String, runsPerQuery: Int, perfSuiteType: String, logUtils: LogUtils, fsUtil: FSUtil): QueryExecutor =
    new QueryExecutor(sparkSession, skipCosmosOLTPQueryExecution, skipCosmosOLAPQueryExecution, cosmosEndpoint, cosmosAccountKey, cosmosRegion, cosmosDatabaseName, cosmosSynapseDatabaseName, scaleFactor, queriesToRun, logUtils.getOutputPath(), runsPerQuery, fsUtil)

}
