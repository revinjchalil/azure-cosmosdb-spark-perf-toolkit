package ms.runners

import com.typesafe.scalalogging.slf4j.LazyLogging

import ms.config
import ms.jobsubmission.{Job, JobStatus, JobSubmitter, JobSubmitterFactory, OptionsParserFactory, ParsedOptions}
import ms.jobsubmission.JobStatus.JobStatus
import ms.util.PerfSuiteDataGeneratorOptions
import ms.util.PerfSuiteOptionsParser

/**
 * Performance Suite runner
 * @param jobSubmitter JobSubmitter interface
 * @param jarFilePath Jar File path
 * @param applicationArgs Argument to spark application
 * @param conf Spark conf
 * @param perfSuiteOptionsParser PerfSuiteOptionsParser
 */
class PerformanceSuiteRunner(jobSubmitter: JobSubmitter,
                             jarFilePath: String,
                             applicationArgs: PerfSuiteDataGeneratorOptions,
                             conf: Map[String, String],
                             perfSuiteOptionsParser: PerfSuiteOptionsParser) extends LazyLogging  {

  def run(): Unit = {
//    invokeLocalValidation()
//    invokeValidationJob()
//    invokePrePerfRunSetup()

    if (!applicationArgs.skipDataGeneration) {
      invokeDataGeneration()
    }

    if (!applicationArgs.skipMetastoreCreation) {
      invokeMetastoreSetup()
    }

    if (!applicationArgs.skipCosmosIngestion) {
      invokeCosmosIngestion()
    }

    if (!applicationArgs.skipCosmosSynapseViewCreation) {
      invokeCosmosSynapseViewSetup()
    }

    if (!applicationArgs.skipCosmosOLTPQueryExecution || !applicationArgs.skipCosmosOLAPQueryExecution) {
      invokeCosmosQueryExecution()
    }

    if (!applicationArgs.skipQueryExecution) {
      invokeQueryExecution()
    }

//    invokeSummaryGeneration()
  }

  private def invokeLocalValidation(): Unit = {
    logger.info("Invoking Local Validation")
    jobSubmitter.localValidation(applicationArgs, conf)
  }

  private def invokeValidationJob(): Unit = {
    logger.info("Invoking Validation")
    val job = Job("Validation Job",
      "ms.validation.RunValidator",
      jarFilePath,
      Array(applicationArgs.skipDataGeneration.toString,
        applicationArgs.skipMetastoreCreation.toString,
        applicationArgs.skipQueryExecution.toString,
        applicationArgs.rootDir,
        applicationArgs.databaseName,
        applicationArgs.dataGenToolkitPath.get),
      conf)
    val jobStatus = jobSubmitter.runJob(job)
    validateSuccess(jobStatus)
  }

  private def invokePrePerfRunSetup(): Unit = {
    logger.info("Invoking Perf run setup")
    var applicationArgsLocal = Array(applicationArgs.metricsOutputPath,
      applicationArgs.tagName,
      applicationArgs.scaleFactor)
    val storageSecretKey = applicationArgs.storageSecretKey
    if (storageSecretKey != null) applicationArgsLocal = applicationArgsLocal :+ storageSecretKey

    val job = Job("PrePerfSetup Job",
      "ms.setup.PerfRunSetup",
      jarFilePath,
      applicationArgsLocal,
      conf)
    val jobStatus = jobSubmitter.runJob(job)
    validateSuccess(jobStatus)
  }

  private def invokeDataGeneration(): Unit = {
    logger.info("Invoking Data Generation.")
    val applicationArgsLocal = applicationArgs.copy(
      skipMetastoreCreation = true, skipQueryExecution = true
    )

    val job = Job("Data Generation Job",
      "ms.datagen.DataGenerator",
      jarFilePath,
      perfSuiteOptionsParser.deserializeParsedOptions(applicationArgsLocal).split("#"),
      conf)
    val jobStatus = jobSubmitter.runJob(job)
    validateSuccess(jobStatus)
  }

  private def invokeMetastoreSetup(): Unit = {
    logger.info("Invoking Metastore Setup")
    val applicationArgsLocal = applicationArgs.copy(
      skipDataGeneration = true, skipQueryExecution = true)
    val job = Job("Metastore Setup Job",
      "ms.metastoresetup.MetastoreSetup",
      jarFilePath,
      perfSuiteOptionsParser.deserializeParsedOptions(applicationArgsLocal).split("#"),
      conf)
    val jobStatus = jobSubmitter.runJob(job)
    validateSuccess(jobStatus)
  }

  private def invokeCosmosIngestion(): Unit = {
    logger.info("Invoking Cosmos Ingestion using OLTP Spark Connector")
    val applicationArgsLocal = applicationArgs.copy(
      skipDataGeneration = true, skipQueryExecution = true)
    val job = Job("Cosmos Ingestion Job",
      "ms.cosmosingestion.CosmosIngestor",
      jarFilePath,
      perfSuiteOptionsParser.deserializeParsedOptions(applicationArgsLocal).split("#"),
      conf)
    val jobStatus = jobSubmitter.runJob(job)
    validateSuccess(jobStatus)
  }

  private def invokeCosmosSynapseViewSetup(): Unit = {
    logger.info("Invoking Cosmos Synapse View Setup")
    val applicationArgsLocal = applicationArgs.copy(
      skipDataGeneration = true, skipQueryExecution = true)
    val job = Job("Cosmos Synapse View Setup Job",
      "ms.cosmossynapseviewsetup.CosmosSynapseViewSetup",
      jarFilePath,
      perfSuiteOptionsParser.deserializeParsedOptions(applicationArgsLocal).split("#"),
      conf)
    val jobStatus = jobSubmitter.runJob(job)
    validateSuccess(jobStatus)
  }

  private def invokeCosmosQueryExecution(): Unit = {
    logger.info("Invoking Cosmos Query Execution")
    val queriesToSkip = applicationArgs.queriesToSkip.split(config.queryNameSeparator).toSet
      val applicationArgsLocal = applicationArgs.copy(
        skipMetastoreCreation = true,
        skipDataGeneration = true)
      val job = Job("Cosmos Query Execution Job",
        "ms.cosmosquery.QueryExecutor",
        jarFilePath,
        perfSuiteOptionsParser.deserializeParsedOptions(applicationArgsLocal).split("#"),
        conf)
      val jobStatus = jobSubmitter.runJob(job)
  }

  private def invokeQueryExecution(): Unit = {
    logger.info("Invoking Query Execution")
    val queriesToSkip = applicationArgs.queriesToSkip.split(config.queryNameSeparator).toSet
    val queriesToRun = applicationArgs.queriesToRun.get.split(
      config.queryNameSeparator).filterNot(queriesToSkip).sorted
    logger.info(s"Queries to Run: ${queriesToRun.mkString(",")}")
    for (queryName <- queriesToRun) {
      logger.info(s"Submitting the job for $queryName")
      val applicationArgsLocal = applicationArgs.copy(
        skipMetastoreCreation = true,
        skipDataGeneration = true,
        queriesToRun = Option(queryName.trim),
        queriesToSkip = "")
      val job = Job("Query Execution Job",
        "ms.query.QueryExecutor",
        jarFilePath,
        perfSuiteOptionsParser.deserializeParsedOptions(applicationArgsLocal).split("#"),
        conf)
      val jobStatus = jobSubmitter.runJob(job)
      logger.info(s"Job for $queryName finished with $jobStatus")
    }
  }

  private def invokeSummaryGeneration(): Unit = {
    logger.info("Invoking summary generation")
    var applicationArgsLocal = Array(applicationArgs.metricsOutputPath,
      applicationArgs.tagName,
      applicationArgs.scaleFactor,
      applicationArgs.runsPerQuery,
      applicationArgs.systemMetricsPort.isDefined.toString)
    val storageSecretKey = applicationArgs.storageSecretKey
    if (storageSecretKey != null) applicationArgsLocal = applicationArgsLocal :+ storageSecretKey

    val job = Job("Summary Generation Job",
      "ms.summary.GenerateSummary",
      jarFilePath,
      applicationArgsLocal,
      conf)
    val jobStatus = jobSubmitter.runJob(job)
    validateSuccess(jobStatus)
  }

  private def validateSuccess(jobStatus: JobStatus) {
    logger.info(s"Job finished with status $jobStatus.")
    if (jobStatus != JobStatus.SUCCEEDED) {
      throw new RuntimeException(s"Job failed with status $jobStatus.")
    }
  }
}

/**
* Companion Object of performance suite runner.
*/
object PerformanceSuiteRunner {
  def apply(parsedOptions: ParsedOptions, jobSubmitter: JobSubmitter): PerformanceSuiteRunner = {
    val perfSuiteOptionsParser: PerfSuiteOptionsParser = PerfSuiteOptionsParser()
    val defaultQueryList = config.getQueryList(
      parsedOptions.applicationArgs.perfSuiteType)
    val defaultDataGenPath = config.getDefaultDataGenPath(
      parsedOptions.applicationArgs.perfSuiteType)
    val applicationArgs = parsedOptions.applicationArgs.copy(
      queriesToRun = Option(parsedOptions.applicationArgs.queriesToRun.getOrElse(defaultQueryList)),
      dataGenToolkitPath = Option(parsedOptions.applicationArgs.dataGenToolkitPath.getOrElse(
        defaultDataGenPath))
    )
    new PerformanceSuiteRunner(jobSubmitter, parsedOptions.jarFilePath, applicationArgs,
      parsedOptions.conf, perfSuiteOptionsParser)
  }

  def main(args: Array[String]): Unit = {
    var jobSubmitter: JobSubmitter = null
    try {
      val confFilePath = args(0)
      val jobSubmitterType =
        if (args.length == 2) JobSubmitter.getJobSubmitterType(args(1).toUpperCase())
        else JobSubmitter.LIVY

      val parsedOptions = OptionsParserFactory().get(jobSubmitterType).parse(confFilePath)
      jobSubmitter = JobSubmitterFactory().get(parsedOptions)

      PerformanceSuiteRunner(parsedOptions, jobSubmitter).run()
    } catch {
      case exception: ArrayIndexOutOfBoundsException =>
        // scalastyle:off println
        exception.printStackTrace()
        println(s"Error parsing the provided cmd args: ${args.mkString(";")}. Check usage.")
      // scalastyle:on println
      case exception: NullPointerException =>
        exception.printStackTrace()
      case unknown => println("Got this unknown exception: " + unknown)
    } finally {
      jobSubmitter.close()
    }
  }
}
