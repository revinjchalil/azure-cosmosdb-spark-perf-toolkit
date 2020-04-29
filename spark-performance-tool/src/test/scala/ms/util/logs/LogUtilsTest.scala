package ms.util.logs

import java.io.IOException

import org.scalatest.{BeforeAndAfterAll, FunSpec, MustMatchers}
import org.scalatestplus.mockito.MockitoSugar

import ms.LocalSparkSession
import ms.util.FSUtil

class LogUtilsTest extends FunSpec
  with MustMatchers
  with BeforeAndAfterAll
  with LocalSparkSession
  with MockitoSugar{

  val outputPath = "/tmp/spark-perf-out"
  val fsUtil = FSUtil(outputPath)
  val tag = "perfTag"
  val scaleFactor = "10"
  val logManager = new LogUtils(fsUtil, outputPath, tag, scaleFactor, spark)
  val secretDbKey = "ABC"
  var outputDir: String = _

  override def beforeAll(): Unit = {
    fsUtil.delete(outputPath)
    spark.conf.set("spark.eventLog.dir", "/tmp/spark-events/")
    val sparkEventLogDir = spark.conf.get("spark.eventLog.dir")
    val appId = spark.sparkContext.applicationId
    fsUtil.writeAsFile(s"$sparkEventLogDir/$appId.inprogress", "Test logs")
  }

  override def afterAll(): Unit = {
    fsUtil.delete(outputPath)
  }

  describe("Test the LogManager") {
    it("Test the output directory setup") {
      outputDir = logManager.getOutputPath()
      logManager.setupOutputDir()
      fsUtil.getFilePathList(outputPath).length must be (1)
      outputDir.startsWith(s"$outputPath/$tag/sf_$scaleFactor") must be (true)
    }

    it("Test the logs being backed up to output dir") {
      fsUtil.getFilePathList(outputDir).length must be (0)
      logManager.backup("query1")
      fsUtil.getFilePathList(outputDir).length must be (1)
      }
    }

  it("Test if mkdir fails when folder already exists") {
    intercept[IOException] {
      fsUtil.mkdir("/tmp/spark-events/")
    }
  }
}
