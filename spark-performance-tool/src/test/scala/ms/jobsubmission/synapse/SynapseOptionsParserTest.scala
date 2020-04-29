package ms.jobsubmission.synapse

import java.io.File

import ms.util.PerfSuiteDataGeneratorOptions
import org.scalatest.FunSpec

class SynapseOptionsParserTest extends FunSpec {
  val inputArgsFile = new File(getClass.getClassLoader.getResource(
    "synapse/sample-conf").getFile).getAbsolutePath

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

  val expectedOutput: SynapseParsedOptions = SynapseParsedOptions(
    jarFilePath = "abfs://some/store/" +
      "spark-performance-toolkit-1.0-SNAPSHOT-jar-with-dependencies.jar",
    applicationArgs = appArgs,
    conf = Map("spark.sql.broadcastTimeout" -> "-1"),
    "synapseUri",
    "synapseSparkPool",
    "tenantId",
    "servicePrincipalId",
    "servicePrincipalSecret")

  val synapseOptionsParser = SynapseOptionsParser()

  describe("Test LivyOptionParser ") {
    it("The livy args should be parsed as expected") {
      val actualOutput = synapseOptionsParser.parse(inputArgsFile)
      assert(actualOutput.equals(expectedOutput))
    }
  }


}
