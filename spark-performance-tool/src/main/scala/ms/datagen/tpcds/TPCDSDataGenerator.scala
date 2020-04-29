package ms.datagen.tpcds

import org.apache.spark.SparkFiles
import org.apache.spark.sql.SparkSession

import ms.datagen.DataGenerator
import ms.util.tpcds.TPCDSTablesWrapper

/**
 * TPCDS Data Generator, generates data in given format and creates TPCDS external tables in the DB.
 *
 * @param sparkSession      spark session
 * @param rootDir           root directory of location to create data in.
 * @param format            valid spark format like parquet "parquet".
 * @param scaleFactor       scaleFactor defines the size of the dataset to generate (in GB).
 * @param dataGenToolkitPath Path to dsdgen directory.
 */
class TPCDSDataGenerator(sparkSession: SparkSession,
                         rootDir: String,
                         format: String,
                         scaleFactor: String,
                         dataGenToolkitPath: Option[String],
                         dataGenPartitionsToRun: String) extends
  DataGenerator(rootDir, format, dataGenPartitionsToRun) {

  override val tables = new TPCDSTablesWrapper(sparkSession.sqlContext,
    dsdgenDir = SparkFiles.get("dsdgen"),
    scaleFactor = scaleFactor,
    useDoubleForDecimal = false,
    useStringForDate = false)

  override def setup(): Unit = {
    sparkSession.sparkContext.addFile(s"${dataGenToolkitPath.get}/dsdgen")
    sparkSession.sparkContext.addFile(s"${dataGenToolkitPath.get}/tpcds.idx")
  }
}

object TPCDSDataGenerator {
  val defaulttpcdsdataGenPath =
    "https://tpcdsperfsetup.blob.core.windows.net/perfsetup/tpcds-kit-tools"
}
