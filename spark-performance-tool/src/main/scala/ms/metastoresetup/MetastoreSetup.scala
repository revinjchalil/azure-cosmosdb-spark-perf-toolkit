package ms.metastoresetup

import java.util.Locale

import com.databricks.spark.sql.perf.Tables
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import ms.common.PerfSuiteType
import ms.util.PerfSuiteOptionsParser

abstract class MetastoreSetup(rootDir: String, databaseName: String, format: String) {

  val tables: Tables = null

  /**
   * set up metastore
   */
  def setupMetastore(): Unit = {
    setup()
    createDB()
    createExternalTables()
  }

  def setup(): Unit

  def createDB(): Unit

  def createExternalTables(): Unit = {
    // Create metastore tables in a specified database for your data.
    // Once tables are created, the current database will be switched to the specified database.
    tables.createExternalTables(
      rootDir,
      format,
      databaseName,
      overwrite = true,
      discoverPartitions = true)
    // Or, if you want to create temporary tables
    // tables.createTemporaryTables(location, format)

    tables.analyzeTables(databaseName, analyzeColumns = true)
  }

}

object MetastoreSetup {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf(true)
    conf.setAppName("MetastoreSetup")
    val sparkSession =
      SparkSession.builder.enableHiveSupport().config(conf).getOrCreate()


    val optionParser = PerfSuiteOptionsParser()
    val argValues = optionParser.parseOptions(args)

    MetastoreSetup(sparkSession,
      argValues.rootDir,
      argValues.databaseName,
      argValues.format,
      argValues.scaleFactor,
      argValues.dataGenToolkitPath,
      argValues.perfSuiteType).setupMetastore()
  }

  def apply(sparkSession: SparkSession,
            rootDir: String,
            databaseName: String,
            format: String,
            scaleFactor: String,
            dataGenToolkitPath: Option[String],
            perfSuiteType: String): MetastoreSetup = {

    MetastoreSetupFactory().get(sparkSession,
      rootDir,
      databaseName,
      format,
      scaleFactor,
      dataGenToolkitPath,
      PerfSuiteType.getPerfSuiteType(perfSuiteType.toUpperCase(Locale.ROOT))
    )
  }
}
