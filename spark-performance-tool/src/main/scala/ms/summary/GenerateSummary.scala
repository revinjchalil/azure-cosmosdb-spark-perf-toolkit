package ms.summary

import java.net.URI

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import scala.io.Source

import ms.util.logs.LogUtils

/**
 * Create a summary csv file for query metrics
 */
class GenerateSummary(spark: SparkSession,
                      logUtils: LogUtils,
                      runsPerQuery: Int) {
  import spark.implicits._

  def getQueryMetricsDataframe(): DataFrame = {
    val basePath = logUtils.getOutputPath()
    val queryMetricsPath = basePath + "/query-metrics"

    val queryMetrics = spark.read.option("basePath", queryMetricsPath).json(
      s"${queryMetricsPath}/*/**")

    val queryMetricsFlattened = queryMetrics.select($"queryId",
      $"runId",
      $"queryResultValidationSuccess",
      $"runtimes.startTime".alias("startTime"),
      $"runTimes.endTime".alias("endTime"),
      $"runTimes.executionTimeMs".alias("executionTimeMs")
    ).as(Query.toString())

    queryMetricsFlattened
  }

  def generateQueryMetricsSummary(): Unit = {

    val basePath = logUtils.getOutputPath()
    val queryMetrics = getQueryMetricsDataframe
    val window = Window.partitionBy("queryId").orderBy("runId")

    val intermediateRepresentation = queryMetrics
      .withColumn("runTimes", collect_list($"executionTimeMs").over(window))
      .withColumn("validationFlags", collect_list($"queryResultValidationSuccess").over(window))
      .groupBy("queryId").agg(max("runTimes").as("runTimes"),
      max("validationFlags").as("validationFlags"))

    val columnNames = col("queryId") +: (0 until runsPerQuery).map(i => col(
      "runTimes")(i).alias(s"runTimeMs_${i + 1}")) ++: (0 until runsPerQuery).map(i =>
      col("validationFlags")(i).alias(s"resultValidation_${i + 1}"))

    val finalRepresentation = intermediateRepresentation.select(
      columnNames: _*
    ).sort(asc("queryId"))

    finalRepresentation.coalesce(1).write.mode(SaveMode.Overwrite)
      .option("header", "true").csv(basePath + "/query-metrics-csv")
  }

  def generateSystemMetricsSummary(): Unit = {

    val basePath = logUtils.getOutputPath()
    val systemMetricsPath = basePath + "/system-metrics"
    val systemMetricsSummaryPath = basePath + "/system-metrics-summary"

    val queryMetrics = getQueryMetricsDataframe
    queryMetrics.createOrReplaceTempView("queryMetrics")

    val metrics = List(("cpuPercentIdle", "percent-idle"),
      ("cpuPercentInterrupt", "percent-interrupt"),
      ("cpuPercentNice", "percent-nice"), ("cpuPercentSoftirq", "percent-softirq"),
      ("cpuPercentSteal", "percent-steal"), ("cpuPercentSystem", "percent-system"),
      ("cpuPercentUser", "percent-user"), ("cpuPercentWait", "percent-wait"),
      ("memoryBuffered", "memory-buffered"), ("memoryCached", "memory-cached"),
      ("memoryFree", "memory-free"), ("memorySlabRecl", "memory-slab_recl"),
      ("memorySlabUnrecl", "memory-slab_unrecl"), ("memoryUsed", "memory-used"),
      ("diskIoMerged", "disk_merged"), ("diskIoOctets", "disk_octets"),
      ("diskIoOps", "disk_ops"), ("diskIoTime", "disk_io_time"),
      ("diskPendingOperations", "pending_operations"), ("diskTime", "disk_time"),
      ("networkOctets", "if_octets"), ("networkPackets", "if_packets"),
      ("networkErrors", "if_errors"))

    metrics.foreach(m => generateSummaryForAMetric(m._1, m._2))

    def generateSummaryForAMetric(metric: String, fileName: String): Unit = {
      val metrics = spark.read.option("basePath", basePath).option("header", true).
        csv(s"${systemMetricsPath}/*/**/***/$fileName").
        withColumn("epochInMilliSeconds", col("epoch")*1000)
      metrics.createOrReplaceTempView(s"$metric")
      val queryFileName = metric + ".sql"
      val queryString = Source.fromInputStream(getClass.getClassLoader.
        getResourceAsStream(s"system-metrics-summary-queries/$queryFileName")
      ).getLines.mkString(" ")
      val finalMetrics = spark.sql(s"$queryString")
      writeDataFrameToCsv(finalMetrics,
        systemMetricsSummaryPath,
        s"${metric}PerQueryRun-csv")
    }
  }


  def writeDataFrameToCsv(metricsPerQueryRun: DataFrame,
                          basePath: String,
                          fileName: String): Unit = {
    metricsPerQueryRun.coalesce(1).write.mode(SaveMode.Overwrite)
      .option("header", "true").csv(basePath + s"/$fileName")
  }
}

object GenerateSummary extends Logging {
  def main(args: Array[String]): Unit = {

    val outputPath = args(0)
    val tagName = args(1)
    val scaleFactor = args(2)
    val runsPerQuery = args(3).toInt
    val isSystemMetricsEnabled = args(4).toBoolean
    val storageSecretKey = if (args.length == 6) args(5) else null

    val conf = new SparkConf(true)
    conf.setAppName("QueryMetricsAndSystemMetricsSummaryGeneration")
    //  Configure spark to read from common storage using secret key.
    if (storageSecretKey != null) {
      val uri = new URI(outputPath)
      conf.set(s"fs.azure.account.key.${uri.getHost}", storageSecretKey)
      conf.set("fs.azure.account.auth.type", "SharedKey")
    }

    val sparkSession =
      SparkSession.builder.enableHiveSupport().config(conf).getOrCreate()

    val generateSummary = GenerateSummary(sparkSession, outputPath, tagName,
      scaleFactor, runsPerQuery,
      storageSecretKey)

    try {
      generateSummary.generateQueryMetricsSummary()
    } catch {
      case e: Exception =>
        logWarning(s"Exception while generating query metrics summary, cause: ${e.getCause}")
    }

    try {
      if (isSystemMetricsEnabled) {
        generateSummary.generateSystemMetricsSummary()
      }
    } catch {
      case e: Exception =>
        logWarning(s"Exception while generating system metrics summary, cause: ${e.getCause}")
    }


  }

  def apply(sparkSession: SparkSession,
            outputPath: String,
            tagName: String,
            scaleFactor: String,
            runsPerQuery: Int,
            storageSecretKey: String):
  GenerateSummary = new GenerateSummary(sparkSession, LogUtils(outputPath, tagName, scaleFactor,
    storageSecretKey, sparkSession), runsPerQuery)
}

case class Query(queryId: String,
                 runId: Int,
                 queryResultValidationSuccess: Boolean,
                 startTime: Long,
                 endTime: Long,
                 executionTimeMs: Long)
