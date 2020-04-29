package ms.validation

import ms.LocalSparkSession
import ms.util.FSUtil
import org.mockito.Matchers.any
import org.mockito.Mockito.when
import org.scalatest.{BeforeAndAfterAll, FunSpec, MustMatchers}
import org.scalatestplus.mockito.MockitoSugar

class RunValidatorTest extends FunSpec
  with MustMatchers
  with BeforeAndAfterAll
  with LocalSparkSession
  with MockitoSugar {

  val rootDir = "rootDir"
  val dbName = "testDb"
  val dataGenToolkitPath = "testdataGenToolkitPath"
  val fsUtil: FSUtil = mock[FSUtil]


  val runValidator1 = new RunValidator(spark,
    true,
    false,
    false,
    rootDir,
    dbName,
    dataGenToolkitPath)

  val runValidator2 = new RunValidator(spark,
    true,
    true,
    false,
    rootDir,
    dbName,
    dataGenToolkitPath)

  override def beforeAll(): Unit = {
    setup()
    when(fsUtil.exists(any[String])).thenReturn(false)
  }

  override def afterAll(): Unit = {
    cleanup()
  }

  private def setup(): Unit = {
    spark.sql(s"create database $dbName")
    spark.sql(s"use $dbName")
    import spark.implicits._
    1 to 25 foreach { i =>
      Seq(1, 2, 3).toDF("key").createOrReplaceTempView(s"table_$i")
    }
  }

  private def cleanup(): Unit = {
    spark.sql(s"drop database $dbName")
  }

  describe("Test RunValidator") {
    it("Test data availability") {
      intercept[IllegalStateException] {
        runValidator1.validate()
      }
    }

    it("Test Metastore availability") {
      intercept[IllegalStateException] {
        runValidator1.validate()
      }
    }
  }
}
