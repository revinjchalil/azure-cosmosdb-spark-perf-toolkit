package ms.jobsubmission.hdi

import java.io.File

import org.scalatest.FunSpec

import ms.util.PerfSuiteDataGeneratorOptions

class HdiOptionsParserTest extends FunSpec {
  val inputArgsFile = new File(getClass.getClassLoader.getResource(
    "hdi/sample-conf").getFile).getAbsolutePath

  val appArgs = PerfSuiteDataGeneratorOptions(
    rootDir = "tpcds1/",
    databaseName = "tpcds1",
    format = "parquet",
    scaleFactor = "1",
    dataGenPartitionsToRun = "60",
    dataGenToolkitPath = Option("abfs://some-store/tmp/new_tools/tpcds-kit-tools/"),
    runsPerQuery = "3",
    metricsOutputPath = "abfs://some-store/tmp/output",
    queriesToRun = Option("query1,query2,query3,query4,query5"),
    queriesToSkip = "query1,query4",
    storageSecretKey = null,
    skipDataGeneration = true,
    skipQueryExecution = false,
    skipMetastoreCreation = true,
    tagName = "sometag",
    perfSuiteType = "TPCDS",
    systemMetricsPort = None,
    skipCosmosIngestion = true,
    skipCosmosSynapseViewCreation = true,
    skipCosmosOLTPQueryExecution = true,
    skipCosmosOLAPQueryExecution = true,
    cosmosSynapseDatabaseName = null,
    cosmosEndpoint = null,
    cosmosAccountKey = null,
    cosmosRegion = null,
    cosmosDatabaseName = null)

  val expectedOutput: HdiParsedOptions = HdiParsedOptions(
    jarFilePath = "abfs://some/store/" +
      "spark-performance-toolkit-1.0-SNAPSHOT-jar-with-dependencies.jar",
    applicationArgs = appArgs,
    conf = Map("spark.sql.broadcastTimeout" -> "-1"),
    livyUri = "https://some/livy/host/endpoint",
    userName = "some-userName",
    userPassword = "some-password")

  val hdiOptionsParser = HdiOptionsParser()

  describe("Test HdiOptionParser ") {
    it("The hdi args should be parsed as expected") {
      val actualOutput = hdiOptionsParser.parse(inputArgsFile)
      assert(actualOutput.equals(expectedOutput))
    }
  }


}
