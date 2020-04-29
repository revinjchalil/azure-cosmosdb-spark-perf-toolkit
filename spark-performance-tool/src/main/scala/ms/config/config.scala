package ms

import java.nio.file.{Files, FileSystems, Path, Paths}
import java.util.Collections

import ms.common.PerfSuiteType
import ms.datagen.tpcds.TPCDSDataGenerator
import ms.query.ibmtpcds.IBMTpcdsQueryExecutor
import ms.query.tpcds.TPCDSQueryExecutor

/**
 * Configurations used throughout the program.
 */
package object config {
  /**
   * Default path where output metrics are stored. The authentication
   * key must be provided to use this storage location.
   */
  private[ms] val defaultMetricsOutputPath =
    "abfs://runresultstorage@tpcdsperfresults.dfs.core.windows.net"

  /**
   * Default path where the pre compiled jar of spark-performance tool is stored
   */
  private[ms] val defaultJarFilePath =
    "https://tpcdsperfsetup.blob.core.windows.net/perfsetup/spark-performance-tool-" +
      "1.0-SNAPSHOT.jar"

  /**
   * Default path of the dsdgen toolkit
   */
  private[ms] val defaulttpcdsdataGenPath =
    "https://tpcdsperfsetup.blob.core.windows.net/perfsetup/tpcds-kit-tools"

  /**
   * Separator used for query names in input args, when specifying the selected queries to run.
   */
  private[ms] val queryNameSeparator = ","

  /**
   * Interval to check for active jobs to finish, waiting to submit the job.
   */
  private[ms] val livyActiveJobPollingIntervalInSeconds = 30

  /**
   * Default value for Skip Data Generation Config.
   */
  private[ms] val defaultForSkipDataGeneration = "true"

  /**
   * Default value for skip Metastore creation.
   */
  private[ms] val defaultForSkipMetatsoreCreation = "true"

  /**
   * Default for skip query execution.
   */
  private[ms] val defaultForSkipQueryExecution = "false"

  /**
   * Default for dsdgen Partitions To Run.
   */
  private[ms] val defaultFordataGenPartitionsToRun = "0"

  /**
   * Number of tables in tpcds.
   */
  private[ms] val tpcdsTableCount = 24

  /**
   * Number of retry to livy api
   */
  private[ms] val livyRetryCount = 4

  def getDefaultDataGenPath(perfSuiteType: String): String = {
    PerfSuiteType.getPerfSuiteType(perfSuiteType) match {
      case PerfSuiteType.TPCDS | PerfSuiteType.IBMTPCDS =>
        TPCDSDataGenerator.defaulttpcdsdataGenPath
      case PerfSuiteType.UNDEFINED => throw new RuntimeException("Invalid Perf Suite Runner.")
    }
  }

  def getQueryList(perfSuiteType: String): String = {

    val queriesPathInResources: String = PerfSuiteType.getPerfSuiteType(perfSuiteType) match {
      case PerfSuiteType.TPCDS => TPCDSQueryExecutor.tpcdsQueriesPathInResources
      case PerfSuiteType.IBMTPCDS => IBMTpcdsQueryExecutor.ibmTpcdsQueriesPathInResources
      case PerfSuiteType.UNDEFINED => throw new RuntimeException("Invalid Perf Suite Runner.")
    }

    val uri = getClass.getClassLoader.getResource(queriesPathInResources).toURI
    var myPath: Path = Paths.get("")
    if (uri.getScheme == "jar") {
      val fileSystem = FileSystems.newFileSystem(uri, Collections.emptyMap[String, Any])
      myPath = fileSystem.getPath(queriesPathInResources)
    } else {
      myPath = Paths.get(uri)
    }
    var queryNameList: List[String] = List()
    val walk = Files.walk(myPath, 1)
    val it = walk.iterator
    while (it.hasNext) {
      val fileName = it.next()
      if (fileName.getFileName.toString != queriesPathInResources.split("/").last) {
        queryNameList = queryNameList :+ fileName.getFileName.toString
      }
    }

    val retVal =
      queryNameList.map(file => file.split('.')(0)).mkString(config.queryNameSeparator)
    retVal
  }
}
