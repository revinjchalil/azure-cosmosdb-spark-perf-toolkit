package ms.metastoresetup.tpcds

import org.apache.spark.SparkFiles
import org.apache.spark.sql.SparkSession

import ms.metastoresetup.MetastoreSetup
import ms.util.tpcds.TPCDSTablesWrapper

/**
 * TPCDS Data Generator, generates data in given format and creates TPCDS external tables in the DB.
 *
 * @param sparkSession      spark session
 * @param rootDir           root directory of location to create data in.
 * @param databaseName      name of database to create.
 * @param format            valid spark format like parquet "parquet".
 * @param scaleFactor       scaleFactor defines the size of the dataset to generate (in GB).
 * @param dataGenToolkitPath Path to dsdgen directory.
 */
class TPCDSMetastoreSetup(sparkSession: SparkSession,
                          rootDir: String,
                          databaseName: String,
                          format: String,
                          scaleFactor: String,
                          dataGenToolkitPath: Option[String]) extends
  MetastoreSetup(rootDir, databaseName, format) {

  override val tables = new TPCDSTablesWrapper(sparkSession.sqlContext,
    dsdgenDir = SparkFiles.get("dsdgen"),
    scaleFactor = scaleFactor,
    useDoubleForDecimal = false,
    useStringForDate = false)

  /**
   * Creates database, drops if already exists.
   */
  override def createDB(): Unit = {
    sparkSession.sql(s"drop database if exists $databaseName cascade")
    sparkSession.sql(s"create database $databaseName")
  }

  override def setup(): Unit = {
    sparkSession.sparkContext.addFile(s"${dataGenToolkitPath.get}/dsdgen")
    sparkSession.sparkContext.addFile(s"${dataGenToolkitPath.get}/tpcds.idx")
  }
}
