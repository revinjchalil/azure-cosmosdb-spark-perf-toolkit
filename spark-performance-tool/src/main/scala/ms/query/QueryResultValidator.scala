package ms.query

import org.apache.spark.internal.Logging
import scala.io.Source

/**
 * Query result validator to validate the result generated by sql queries is same as expected.
 * @param expectedResultsPath path to the folder which contains expected output of the queries.
 */
class QueryResultValidator(expectedResultsPath: String) extends Logging {

  def validate(queryName: String, queryResult: Array[org.apache.spark.sql.Row]): Boolean = {
    val expectedOutputPath = getExpectedResultsPath + s"/$queryName.sql"
    var expectedOutput: String = ""
    try {
      expectedOutput = getFileFromResources(expectedOutputPath)
    } catch {
      case exception: IllegalArgumentException => logError("Error encountered while fetching the " +
        "expected output for the query." + exception.printStackTrace() )
    }

    val actualOutput = queryResult.map(_.toSeq).mkString(" ")
    if(expectedOutput != actualOutput) {
      logError(s"The output doesn't match for the $queryName. " +
        s"\n The actual output is:\n$actualOutput" +
        s"\n The expected output is:\n$expectedOutput" )
      return false
    }
    true
  }

  def getExpectedResultsPath: String = expectedResultsPath

  def getFileFromResources(fileName: String): String = {
    val resource = getClass.getClassLoader.getResourceAsStream(fileName)
    if (resource == null) throw new IllegalArgumentException(s"file $fileName is not found in " +
      s"resources!")
    else {
      Source.fromInputStream(resource).getLines.mkString
    }
  }

}

object QueryResultValidator{
  def apply(expectedResultsPath: String): QueryResultValidator =
    new QueryResultValidator(expectedResultsPath)
}