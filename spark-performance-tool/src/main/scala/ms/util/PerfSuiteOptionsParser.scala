package ms.util

import java.text.SimpleDateFormat
import java.util.{Collections, Date}

import org.apache.commons.cli.{BasicParser, Options}
import scala.util.Try

import ms.common.PerfSuiteType
import ms.config


/**
 * Perf Suite Options Parser class to define and parse cli arguments.
 * @param options options.
 * @param parser  options parser.
 */
class PerfSuiteOptionsParser(options: Options, parser: BasicParser) {
  val rootDirParamName = "rootDir"
  val databaseParamName = "databaseName"
  val formatParamName = "format"
  val scaleFactorParamName = "scaleFactor"
  val dataGenToolKitPathParamName = "dataGenToolkitPath"
  val metricsOutputPathParamName = "metricsOutputPath"
  val queriesToRunParamName = "queriesToRun"
  val queriesToSkipParamName = "queriesToSkip"
  val runsPerQueryParamName = "runsPerQuery"
  val skipDataGenerationParamName = "skipDataGeneration"
  val skipMetastoreCreationParamName = "skipMetastoreCreation"
  val skipQueryExecutionParamName = "skipQueryExecution"
  val dataGenPartitionsToRunParamName = "dataGenPartitionsToRun"
  val storageSecretKeyParamName = "storageSecretKey"
  val tagNameParamName = "tagName"
  val perfSuiteTypeParamName = "perfSuiteType"
  val systemMetricsPortParamName = "systemMetricsPort"

  val skipCosmosIngestionParamName = "skipCosmosIngestion"
  val skipCosmosSynapseViewCreationParamName = "skipCosmosSynapseViewCreation"
  val skipCosmosOLTPQueryExecutionParamName = "skipCosmosOLTPQueryExecution"
  val skipCosmosOLAPQueryExecutionParamName = "skipCosmosOLAPQueryExecution"
  val cosmosSynapseDatabaseNameParamName = "cosmosSynapseDatabaseName"
  val cosmosEndpointParamName = "cosmosEndpoint"
  val cosmosAccountKeyParamName = "cosmosAccountKey"
  val cosmosRegionParamName = "cosmosRegion"
  val cosmosDatabaseNameParamName = "cosmosDatabaseName"

//  val skipCosmosDBIngestionParamName = "skipCosmosDBIngestion"
//  val skipCosmosDBQueryParamName = "skipCosmosDBQuery"

  options.addOption(
    "r",
    rootDirParamName,
    true,
    "root directory of location to create data in."
  ).addOption(
      "d",
    databaseParamName,
    true,
    "name of database to create."
  ).addOption(
    "f",
    formatParamName,
    true,
    "valid spark format like parquet."
  ).addOption(
    "s",
    scaleFactorParamName,
    true,
    "scale factor, 1 is equivalent to 1GB of data."
  ).addOption(
    "d",
    dataGenToolKitPathParamName,
    true,
    "Path where dataGen toolkit has be downloaded."
  ).addOption(
    "m",
    metricsOutputPathParamName,
    true,
    "Path where the metrics like execution time etc will be output."
  ).addOption(
    "sk",
    skipDataGenerationParamName,
    true,
    "skip data generation and read from input database."
  ).addOption(
    "qr",
    queriesToRunParamName,
    true,
    "Comma separated query ids which should be run."
  ).addOption(
    "qs",
    queriesToSkipParamName,
    true,
    "Comma separated query ids which should be excluded from run."
  ).addOption(
    "ru",
    runsPerQueryParamName,
    true,
    "Number of times each query must be run."
  ).addOption(
    "se",
    skipMetastoreCreationParamName,
    true,
    "id set to true, set up metastore from uisng rootDir."
  ).addOption(
    "ds",
    dataGenPartitionsToRunParamName,
    true,
    "dataGen partitions to run, number of input tasks."
  ).addOption(
    "sk",
    storageSecretKeyParamName,
    true,
    "Secret key for the storage account."
  ).addOption(
    "tn",
    tagNameParamName,
    true,
    "Used to tag the results for easier identification."
  ).addOption(
    "qexec",
    skipQueryExecutionParamName,
    true,
    "if true skip query execution"
  ).addOption(
    "pstype",
    perfSuiteTypeParamName,
    true,
    "Perf suite to run"
  ).addOption(
    "sms",
    systemMetricsPortParamName,
    true,
    "http port of system metrics server, this is the port at which you have started the " +
      "system metrics server"
  ).addOption(
    "ci",
    skipCosmosIngestionParamName,
    true,
    "Skip ingestion to Cosmos DB"
  ).addOption(
    "csv",
    skipCosmosSynapseViewCreationParamName,
    true,
    "Skip View creation in Synapse pointing to Cosmos DB"
  ).addOption(
    "ctq",
    skipCosmosOLTPQueryExecutionParamName,
    true,
    "Skip executing queries against Cosmos OLTP views"
  ).addOption(
    "caq",
    skipCosmosOLAPQueryExecutionParamName,
    true,
    "Skip executing queries against Cosmos OLAP views"
  ).addOption(
    "csd",
    cosmosSynapseDatabaseNameParamName,
    true,
    "Database Name in Synapse for Cosmos DB Views"
  ).addOption(
    "ce",
    cosmosEndpointParamName,
    true,
    "cosmos DB Endpoint"
  ).addOption(
    "ck",
    cosmosAccountKeyParamName,
    true,
    "cosmos DB AccountKey"
  ).addOption(
    "cr",
    cosmosRegionParamName,
    true,
    "cosmos DB Region"
  ).addOption(
    "cq",
    cosmosDatabaseNameParamName,
    true,
    "cosmos DB Database Name"
  )

  /**
   * Parses the cli arguments.
   * @param args cli arguments
   * @return parsed PerfSuiteDataGeneratorOptions.
   */
  def parseOptions(args: Array[String]): PerfSuiteDataGeneratorOptions = {
    val cmd = parser.parse(options, args)

    val perfSuiteType = cmd.getOptionValue(perfSuiteTypeParamName, PerfSuiteType.TPCDS.toString)

    PerfSuiteDataGeneratorOptions(cmd.getOptionValue(rootDirParamName),
      cmd.getOptionValue(databaseParamName),
      cmd.getOptionValue(formatParamName),
      cmd.getOptionValue(scaleFactorParamName),
      Option(cmd.getOptionValue(dataGenToolKitPathParamName)),
      cmd.getOptionValue(metricsOutputPathParamName, config.defaultMetricsOutputPath),
      cmd.getOptionValue(skipDataGenerationParamName,
        config.defaultForSkipDataGeneration).toBoolean,
      Option(cmd.getOptionValue(queriesToRunParamName)),
      cmd.getOptionValue(queriesToSkipParamName, ""),
      cmd.getOptionValue(runsPerQueryParamName),
      cmd.getOptionValue(skipMetastoreCreationParamName,
        config.defaultForSkipMetatsoreCreation).toBoolean,
      cmd.getOptionValue(skipQueryExecutionParamName,
        config.defaultForSkipQueryExecution).toBoolean,
      cmd.getOptionValue(dataGenPartitionsToRunParamName, config.defaultFordataGenPartitionsToRun),
      cmd.getOptionValue(storageSecretKeyParamName),
      cmd.getOptionValue(tagNameParamName, s"default_${getCurrentTimestamp()}"),
      perfSuiteType,
      Try( cmd.getOptionValue(systemMetricsPortParamName).toInt ).toOption,
      cmd.getOptionValue(skipCosmosIngestionParamName).toBoolean,
      cmd.getOptionValue(skipCosmosSynapseViewCreationParamName).toBoolean,
      cmd.getOptionValue(skipCosmosOLTPQueryExecutionParamName).toBoolean,
      cmd.getOptionValue(skipCosmosOLAPQueryExecutionParamName).toBoolean,
      cmd.getOptionValue(cosmosSynapseDatabaseNameParamName),
      cmd.getOptionValue(cosmosEndpointParamName),
      cmd.getOptionValue(cosmosAccountKeyParamName),
      cmd.getOptionValue(cosmosRegionParamName),
      cmd.getOptionValue(cosmosDatabaseNameParamName)
    )
  }

  def deserializeParsedOptions(parsedOptions: PerfSuiteDataGeneratorOptions): String = {
    var returnValue = s"--$rootDirParamName#${parsedOptions.rootDir}#" +
      s"--$databaseParamName#${parsedOptions.databaseName}#" +
      s"--$formatParamName#${parsedOptions.format}#" +
      s"--$scaleFactorParamName#${parsedOptions.scaleFactor}#" +
      s"--$skipDataGenerationParamName#${parsedOptions.skipDataGeneration}#" +
      s"--$queriesToSkipParamName#${parsedOptions.queriesToSkip}#" +
      s"--$runsPerQueryParamName#${parsedOptions.runsPerQuery}#" +
      s"--$skipMetastoreCreationParamName#${parsedOptions.skipMetastoreCreation}#" +
      s"--$skipQueryExecutionParamName#${parsedOptions.skipQueryExecution}#" +
      s"--$dataGenPartitionsToRunParamName#${parsedOptions.dataGenPartitionsToRun}#" +
      s"--$skipCosmosIngestionParamName#${parsedOptions.skipCosmosIngestion}#" +
      s"--$skipCosmosSynapseViewCreationParamName#${parsedOptions.skipCosmosSynapseViewCreation}#" +
      s"--$skipCosmosOLTPQueryExecutionParamName#${parsedOptions.skipCosmosOLTPQueryExecution}#" +
      s"--$skipCosmosOLAPQueryExecutionParamName#${parsedOptions.skipCosmosOLAPQueryExecution}#" +
      s"--$cosmosSynapseDatabaseNameParamName#${parsedOptions.cosmosSynapseDatabaseName}#" +
      s"--$cosmosEndpointParamName#${parsedOptions.cosmosEndpoint}#" +
      s"--$cosmosAccountKeyParamName#${parsedOptions.cosmosAccountKey}#" +
      s"--$cosmosRegionParamName#${parsedOptions.cosmosRegion}#" +
      s"--$cosmosDatabaseNameParamName#${parsedOptions.cosmosDatabaseName}#" +
      s"--$perfSuiteTypeParamName#${parsedOptions.perfSuiteType}#"

    if(parsedOptions.storageSecretKey != null) {
      returnValue += s"--$storageSecretKeyParamName#${parsedOptions.storageSecretKey}#"
    }
    if(parsedOptions.tagName != null) {
      returnValue += s"--$tagNameParamName#${parsedOptions.tagName}#"
    }
    if(parsedOptions.metricsOutputPath != null) {
      returnValue += s"--$metricsOutputPathParamName#${parsedOptions.metricsOutputPath}#"
    }
    if(parsedOptions.systemMetricsPort.isDefined) {
      returnValue += s"--$systemMetricsPortParamName#${parsedOptions.systemMetricsPort.get}#"
    }
    if(parsedOptions.dataGenToolkitPath.isDefined) {
      returnValue += s"--$dataGenToolKitPathParamName#${parsedOptions.dataGenToolkitPath.get}#"
    }
    if(parsedOptions.queriesToRun.isDefined) {
      returnValue += s"--$queriesToRunParamName#${parsedOptions.queriesToRun.get}"
    }
    returnValue
  }

  private def getCurrentTimestamp(): String = {
    new SimpleDateFormat("YYYYMMdd_HHmmss").format(new Date())
  }
}

/**
 * Companion object.
 */
object PerfSuiteOptionsParser {
  val options = new Options
  val parser = new BasicParser
  def apply(): PerfSuiteOptionsParser = new PerfSuiteOptionsParser(options, parser)
}

/**
 * Case for defining the cli options for Perf Suite Data Generator.
 *
 * @param rootDir
 * @param databaseName
 * @param format
 * @param scaleFactor
 * @param dataGenToolkitPath
 * @param metricsOutputPath
 * @param skipDataGeneration
 * @param queriesToRun comma separated query ids which should be executed
 * @param queriesToSkip comma separated query ids for which the execution should be skipped
 * @param runsPerQuery
 * @param skipMetastoreCreation
 * @param skipQueryExecution
 * @param dataGenPartitionsToRun
 * @param storageSecretKey
 * @param tagName
 * @param perfSuiteType
 * @param systemMetricsPort
 * @param skipCosmosIngestion
 * @param skipCosmosSynapseViewCreation
 * @param skipCosmosOLTPQueryExecution
 * @param skipCosmosOLAPQueryExecution
 * @param cosmosSynapseDatabaseName
 * @param cosmosEndpoint
 * @param cosmosAccountKey
 * @param cosmosRegion
 * @param cosmosDatabaseName
 */
case class PerfSuiteDataGeneratorOptions(rootDir: String,
                                         databaseName: String,
                                         format: String,
                                         scaleFactor: String,
                                         dataGenToolkitPath: Option[String],
                                         metricsOutputPath: String,
                                         skipDataGeneration: Boolean,
                                         queriesToRun: Option[String],
                                         queriesToSkip: String,
                                         runsPerQuery: String,
                                         skipMetastoreCreation: Boolean,
                                         skipQueryExecution: Boolean,
                                         dataGenPartitionsToRun: String,
                                         storageSecretKey: String,
                                         tagName: String,
                                         perfSuiteType: String,
                                         systemMetricsPort: Option[Int],
                                         skipCosmosIngestion: Boolean,
                                         skipCosmosSynapseViewCreation: Boolean,
                                         skipCosmosOLTPQueryExecution: Boolean,
                                         skipCosmosOLAPQueryExecution: Boolean,
                                         cosmosSynapseDatabaseName: String,
                                         cosmosEndpoint:String,
                                         cosmosAccountKey: String,
                                         cosmosRegion: String,
                                         cosmosDatabaseName: String
                                        )
