package ms.validation

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import ms.config
import ms.util.FSUtil

/**
 * To validate each run before moving ahead to run it.
 *
 * @param sparkSession           spark session.
 * @param skipDataGeneration     flag to skip data generation.
 * @param skipMetastoreCreation  flag to skip Metastore creation.
 * @param skipQueryExecution     flag to skip query execution.
 * @param rootDir                root directory of location to create data in.
 * @param databaseName           name of database to create.
 * @param dataGenToolkitPath     Path to dsdgen directory.
 */

class RunValidator(sparkSession: SparkSession,
                   skipDataGeneration: Boolean,
                   skipMetastoreCreation: Boolean,
                   skipQueryExecution: Boolean,
                   rootDir: String,
                   databaseName: String,
                   dataGenToolkitPath: String) {

  def validate(): Unit = {
    justifyFlags()
    validatePaths()
  }

  /**
   * This function validates flag values and checks for existence of directories
   * and tables to satisfy these flags.
   */
  private def justifyFlags(): Unit = {

    val fsUtil = FSUtil(rootDir)

    if (skipDataGeneration && !skipMetastoreCreation && !fsUtil.exists(rootDir)) {
      throw new IllegalStateException(s"Data path $rootDir doesn't exist.")
    }

    if(skipDataGeneration && skipMetastoreCreation && !skipQueryExecution) {
      if (!metastoreExists()) {
        throw new IllegalStateException(s"Metastore $databaseName doesn't exist.")
      }
    }
  }

  /**
   * directory validation.
   */
  private def validatePaths(): Unit = {
    val errorMessage = "Path doesn't exist: %s"
    if(!skipDataGeneration) {
      val fsUtil = FSUtil(dataGenToolkitPath)
      if (!fsUtil.exists(dataGenToolkitPath)) {
        throw new IllegalArgumentException(errorMessage.format(dataGenToolkitPath))
      }
    }
  }

  private def metastoreExists(): Boolean = {
    if (sparkSession.catalog.databaseExists(databaseName) &&
        (sparkSession.catalog.listTables(databaseName).count() == config.tpcdsTableCount)) {
      true
    } else {
      false
    }
  }
}

object RunValidator {
  def apply(sparkSession: SparkSession, skipDataGeneration: Boolean, skipMetastoreCreation: Boolean,
            skipQueryExecution: Boolean, rootDir: String, databaseName: String,
            dataGenToolkitPath: String): RunValidator = {
    new RunValidator(sparkSession, skipDataGeneration, skipMetastoreCreation, skipQueryExecution,
      rootDir, databaseName, dataGenToolkitPath)
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf(true)
    conf.setAppName("PerfRunValidation")
    val sparkSession =
      SparkSession.builder.enableHiveSupport().config(conf).getOrCreate()

    val skipDataGeneration = args(0).toBoolean
    val skipMetastoreCreation = args(1).toBoolean
    val skipQueryExecution = args(2).toBoolean
    val rootDir = args(3)
    val databaseName = args(4)
    val dataGenToolkitPath = args(5)

    RunValidator(sparkSession, skipDataGeneration, skipMetastoreCreation, skipQueryExecution,
      rootDir, databaseName, dataGenToolkitPath).validate()
  }
}
