package ms.summary

import java.io.File
import java.nio.file.Files
import java.nio.file.Paths

import org.mockito.Mockito.when
import org.scalatest.BeforeAndAfterAll
import org.scalatest.FunSpec
import org.scalatest.MustMatchers
import org.scalatestplus.mockito.MockitoSugar

import ms.LocalSparkSession
import ms.util.logs.LogUtils

class GenerateSummaryTest extends FunSpec
  with MustMatchers
  with BeforeAndAfterAll
  with LocalSparkSession
  with MockitoSugar {

  val outputPath: String =
    new File(getClass.getClassLoader.getResource("output/tag/sf_1").getFile).getAbsolutePath
  val tagName = "tag"
  val scaleFactor = "1"
  val storageSecretKey = "testSecretKey"
  val runsPerQuery = 2
  val dbName = "testDbName"
  val logUtils: LogUtils = mock[LogUtils]


  val generateSummary = new GenerateSummary(spark,
    logUtils,
    runsPerQuery)

  override def beforeAll(): Unit = {
    setup()
    when(logUtils.getOutputPath()).thenReturn(outputPath)
  }

  private def setup(): Unit = {
    spark.sql(s"create database $dbName")
    spark.sql(s"use $dbName")
  }

  override def afterAll(): Unit = {
    cleanup()
  }

  private def cleanup(): Unit = {
    spark.sql(s"drop database $dbName")
  }

  describe("Test Summary Generation") {
    it("Test query metrics summary generation") {
      generateSummary.generateQueryMetricsSummary()
      val queryMetricsCsvPath = outputPath + "/query-metrics-csv"
      val path = Paths.get(queryMetricsCsvPath)
      val fileList = Files.list(path)
      fileList.count() must be
      2
    }

    it("Test system metrics summary generation") {
      generateSummary.generateSystemMetricsSummary()
      val systemMetricsFolder = outputPath + "/system-metrics-summary"
      val path = Paths.get(systemMetricsFolder)
      val fileList = Files.list(path)
      fileList.count() must equal (23)
      val cpuPercentIdleFilePath = Paths.get(systemMetricsFolder + "/cpuPercentIdlePerQueryRun-csv")
      val df = spark.read.format("csv").option("header",true).load(cpuPercentIdleFilePath.toString)
      df.count() must equal (12)
    }
  }
}
