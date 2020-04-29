package ms.util

import org.scalatest.BeforeAndAfter
import org.scalatest.FunSpec
import org.scalatest.MustMatchers

class PerfSuiteOptionsParserTest extends FunSpec
  with MustMatchers
  with BeforeAndAfter{

  var optionsParserTest: PerfSuiteOptionsParser = _

  val rootDir = "some/root/path"
  val databaseName = "test_DB"
  val format = "parquet"
  val scaleFactor = "10"
  val dataGenToolkitPath = "/some/dsdgen/toolkit"
  val metricsOutputPath = "/some/output/path"
  val skipDataGen = "true"
  val queriesToRun = "query1, query2, query3, query4, query5"
  val queriesToSkip = "query1, query5"
  val runsPerQuery = "3"
  val setupMetastore = "true"
  val dsdgenPartitions = "10"
  val tagName = "perfTag"
  val storageSecretKey = "ABCD"
  val defaultMetricsOutputPath = "abfs://runresultstorage@tpcdsperfresults.dfs.core.windows.net"
  val skipQueryExecution = "false"
  val perfSuiteType = "TPCDS"
  val systemMetricsPortParamName = 9005
  val someSystemMetricsPortParamName = Some(systemMetricsPortParamName)
  val skipCosmosIngestion = true
  val skipCosmosSynapseViewCreation = "true"
  val skipCosmosOLTPQueryExecution  = "true"
  val skipCosmosOLAPQueryExecution = "true"
  val cosmosSynapseDatabaseName = null
  val cosmosEndpoint = null
  val cosmosAccountKey = null
  val cosmosRegion = null
  val cosmosDatabaseName = null

  before{
    optionsParserTest = PerfSuiteOptionsParser()
  }

  describe("test the parsing of the arguments") {
    it("should correctly parse the arguments given") {

      val args: Array[String] = Array(
        s"--${optionsParserTest.rootDirParamName}",
        s"$rootDir",
        s"--${optionsParserTest.scaleFactorParamName}",
        s"$scaleFactor",
        s"--${optionsParserTest.formatParamName}",
        s"$format",
        s"--${optionsParserTest.databaseParamName}",
        s"$databaseName",
        s"--${optionsParserTest.dataGenToolKitPathParamName}",
        s"$dataGenToolkitPath",
        s"--${optionsParserTest.metricsOutputPathParamName}",
        s"$metricsOutputPath",
        s"--${optionsParserTest.skipDataGenerationParamName}",
        s"$skipDataGen",
        s"--${optionsParserTest.queriesToRunParamName}",
        s"$queriesToRun",
        s"--${optionsParserTest.queriesToSkipParamName}",
        s"$queriesToSkip",
        s"--${optionsParserTest.skipMetastoreCreationParamName}",
        s"$setupMetastore",
        s"--${optionsParserTest.runsPerQueryParamName}",
        s"$runsPerQuery",
        s"--${optionsParserTest.dataGenPartitionsToRunParamName}",
        s"$dsdgenPartitions",
        s"--${optionsParserTest.tagNameParamName}",
        s"$tagName",
        s"--${optionsParserTest.storageSecretKeyParamName}",
        s"$storageSecretKey",
        s"--${optionsParserTest.skipQueryExecutionParamName}",
        s"$skipQueryExecution",
        s"--${optionsParserTest.perfSuiteTypeParamName}",
        s"$perfSuiteType",
        s"--${optionsParserTest.systemMetricsPortParamName}",
        s"$systemMetricsPortParamName",
        s"--${optionsParserTest.skipCosmosSynapseViewCreationParamName}",
        s"$skipCosmosIngestion",
        s"--${optionsParserTest.skipCosmosIngestionParamName}",
        s"$skipCosmosSynapseViewCreation",
        s"--${optionsParserTest.skipCosmosSynapseViewCreationParamName}",
        s"$skipCosmosOLTPQueryExecution",
        s"--${optionsParserTest.skipCosmosOLAPQueryExecutionParamName}",
        s"$skipCosmosOLAPQueryExecution",
        s"--${optionsParserTest.cosmosSynapseDatabaseNameParamName}",
        s"$cosmosSynapseDatabaseName",
        s"--${optionsParserTest.cosmosEndpointParamName}",
        s"$cosmosEndpoint",
        s"--${optionsParserTest.cosmosAccountKeyParamName}",
        s"$cosmosAccountKey",
        s"--${optionsParserTest.cosmosRegionParamName}",
        s"$cosmosDatabaseName",
        s"--${optionsParserTest.cosmosDatabaseNameParamName}",
        s"$cosmosDatabaseName"
      )

      val parsedOptions = optionsParserTest.parseOptions(args)
//      val expectedOptions = PerfSuiteDataGeneratorOptions(rootDir, scaleFactor, format, databaseName,
//        Option(dataGenToolkitPath), metricsOutputPath, skipDataGen.toBoolean,
//          Option(queriesToRun), queriesToSkip, skipMetastoreCreation, setupMetastore, runsPerQuery,
//        skipQueryExecution.toBoolean, dsdgenPartitions, storageSecretKey, tagName, perfSuiteType,
//        someSystemMetricsPortParamName, skipCosmosIngestion.toBoolean, skipCosmosSynapseViewCreation.toBoolean,
//        skipCosmosOLTPQueryExecution.toBoolean, skipCosmosOLAPQueryExecution.toBoolean, cosmosSynapseDatabaseName,
//        cosmosEndpoint, cosmosAccountKey, cosmosRegion, cosmosDatabaseName)
//
//      parsedOptions must be(expectedOptions)

    }

    it("should correctly parse with default arguments") {
      val args: Array[String] = Array(
        s"--${optionsParserTest.rootDirParamName}",
        s"$rootDir",
        s"--${optionsParserTest.scaleFactorParamName}",
        s"$scaleFactor",
        s"--${optionsParserTest.formatParamName}",
        s"$format",
        s"--${optionsParserTest.databaseParamName}",
        s"$databaseName",
        s"--${optionsParserTest.dataGenToolKitPathParamName}",
        s"$dataGenToolkitPath",
        s"--${optionsParserTest.skipDataGenerationParamName}",
        s"$skipDataGen",
        s"--${optionsParserTest.queriesToRunParamName}",
        s"$queriesToRun",
        s"--${optionsParserTest.skipMetastoreCreationParamName}",
        s"$setupMetastore",
        s"--${optionsParserTest.runsPerQueryParamName}",
        s"$runsPerQuery",
        s"--${optionsParserTest.dataGenPartitionsToRunParamName}",
        s"$dsdgenPartitions",
        s"--${optionsParserTest.skipQueryExecutionParamName}",
        s"$skipQueryExecution",
        s"--${optionsParserTest.skipCosmosSynapseViewCreationParamName}",
        s"$skipCosmosSynapseViewCreation",
        s"--${optionsParserTest.cosmosSynapseDatabaseNameParamName}",
        s"$cosmosSynapseDatabaseName",
        s"--${optionsParserTest.cosmosEndpointParamName}",
        s"$cosmosEndpoint",
        s"--${optionsParserTest.cosmosAccountKeyParamName}",
        s"$cosmosAccountKey",
        s"--${optionsParserTest.cosmosRegionParamName}",
        s"$cosmosRegion",
        s"--${optionsParserTest.cosmosDatabaseNameParamName}",
        s"$cosmosDatabaseName"
      )

      val parsedOptions = optionsParserTest.parseOptions(args)
      val actualTagName = parsedOptions.tagName
//      val expectedOptions = PerfSuiteDataGeneratorOptions(rootDir, databaseName, format,
//        scaleFactor, Option(dataGenToolkitPath), defaultMetricsOutputPath, skipDataGen.toBoolean,
//        Option(queriesToRun), "", runsPerQuery, setupMetastore.toBoolean,
//        skipQueryExecution.toBoolean, dsdgenPartitions, null, actualTagName, perfSuiteType, Option(systemMetricsPortParamName),
//        skipCosmosSynapseViewCreation.toBoolean, skipCosmosOLTPQueryExecution.toBoolean, skipCosmosOLAPQueryExecution.toBoolean,
//        cosmosSynapseDatabaseName, cosmosEndpoint, cosmosAccountKey, cosmosRegion, cosmosDatabaseName)
//      assert(actualTagName.startsWith("default_"))
//      parsedOptions must be(expectedOptions)
    }
  }

}
