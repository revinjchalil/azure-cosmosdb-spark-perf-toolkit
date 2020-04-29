package ms.query

import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.{BeforeAndAfterAll, FunSpec, MustMatchers}
import org.scalatestplus.mockito.MockitoSugar
import ms.LocalSparkSession
import ms.common.PerfSuiteType
import ms.util.FSUtil

class QueryExecutorTest extends FunSpec
  with MustMatchers
  with BeforeAndAfterAll
  with LocalSparkSession
  with MockitoSugar {
  val dbName = "testdb"
  val scaleFactor = "1" // it is not used as we have mocked queryResultValidator
  val outputPath = "/tmp/spark-per-test"
  val testTable = "test"
  val query1Name = "query1"
  val query2Name = "query2"
  val fsUtil = mock[FSUtil]
  val queryResultValidator = mock[QueryResultValidator]
  val queryExecutor1 = QueryExecutorFactory().get(spark,
    dbName,
    scaleFactor,
    s"$query1Name,$query2Name",
    outputPath,
    2,
    fsUtil,
    PerfSuiteType.TPCDS)

  val queryExecutor2 = QueryExecutorFactory().get(spark,
    dbName,
    scaleFactor,
    s"$query1Name,$query2Name",
    outputPath,
    2,
    fsUtil,
    PerfSuiteType.IBMTPCDS)

  override def beforeAll(): Unit = {
    setup()
    doNothing().when(fsUtil).writeAsFile(any[String], any[String])
    when(queryResultValidator.validate(any[String], anyObject())).thenReturn(true)
  }

  override def afterAll(): Unit = {
    cleanup()
  }

  private def setup(): Unit = {
    spark.sql(s"create database $dbName")
    spark.sql(s"use $dbName")
    import spark.implicits._
    val s = Seq(1, 2, 3).toDF("key").createOrReplaceTempView(testTable)
  }

  private def cleanup(): Unit = {
    spark.sql(s"drop view $testTable")
    spark.sql(s"drop database $dbName")
  }

  describe("Test Query Executor") {
    it("Test all tpcds queries are executed and logged") {
      val metrics = queryExecutor1.execute()
      metrics.length must be (2*2)
      metrics(0).queryId must be (query1Name)
      metrics(1).queryId must be (query1Name)
      metrics(2).queryId must be (query2Name)
      metrics(3).queryId must be (query2Name)
    }

    it("Test all ibm tpcds queries are executed and logged") {
      val metrics = queryExecutor2.execute()
      metrics.length must be (2*2)
      metrics(0).queryId must be (query1Name)
      metrics(1).queryId must be (query1Name)
      metrics(2).queryId must be (query2Name)
      metrics(3).queryId must be (query2Name)
    }
  }
}
