package ms.query

import org.apache.spark.sql.Row
import org.mockito.Mockito._
import org.scalatest.{BeforeAndAfterAll, FunSpec, MustMatchers}
import org.scalatestplus.mockito.MockitoSugar

import ms.LocalSparkSession

class QueryResultValidatorTest extends FunSpec
  with MustMatchers
  with BeforeAndAfterAll
  with LocalSparkSession
  with MockitoSugar{

  val actualResult = Array(Row.fromSeq(Seq(1, "abc")), Row.fromSeq(Seq(1, "def")))
  val correctOutput = actualResult.map(_.toSeq).mkString(" ")
  val incorrectOutput = Array(Row.fromSeq(Seq(1, "abc")), Row.fromSeq(Seq(2, "def")))
  val queryName1 = "samplequery"
  val queryName2 = "somefilename"

  val queryResultValidator = mock[QueryResultValidator]
  override def beforeAll: Unit = {
    when(queryResultValidator.getExpectedResultsPath).thenReturn("query-validator")
    when(queryResultValidator.getFileFromResources(s"query-validator/$queryName1.sql")
    ).thenReturn(correctOutput)
    when(queryResultValidator.getFileFromResources(s"query-validator/$queryName2.sql")).thenThrow(
      new IllegalArgumentException(s"Mocked: file $queryName2 is not found in resources!"))
    when(queryResultValidator.validate(queryName1, actualResult)).thenCallRealMethod()
    when(queryResultValidator.validate(queryName1, incorrectOutput)).thenCallRealMethod()
    when(queryResultValidator.validate(queryName2, actualResult)).thenCallRealMethod()
  }

  describe("Test the query result validator") {

    it("The validation passes for correct output") {
      queryResultValidator.validate(queryName1, actualResult) must be  (true)
    }

    it("The validation fails for incorrect output") {
      queryResultValidator.validate(queryName1, incorrectOutput) must be  (false)
    }

    it("The validation fails if the expected output file is not present") {
      queryResultValidator.validate(queryName2, actualResult) must be  (false)
    }

  }

}
