package ms.datagen

import java.util.Locale

import com.databricks.spark.sql.perf.Tables
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import ms.common.PerfSuiteType
import ms.util.PerfSuiteOptionsParser

abstract class DataGenerator(rootDir: String, format: String, dataGenPartitionsToRun: String) {

  val tables: Tables = null

  /**
   * generate data
   */
  def generate(): Unit = {
    setup()
    generateData()
  }

  def setup(): Unit

  def generateData(): Unit = {
    tables.genData(
      location = rootDir,
      format = format,
      // overwrite the data that is already there
      overwrite = true,
      // create the partitioned fact tables
      partitionTables = true,
      // shuffle to get partitions coalesced into single files.
      clusterByPartitionColumns = true,
      // true to filter out the partition with NULL key value
      filterOutNullPartitionValues = false,
      // "" means generate all tables
      tableFilter = "",
      // how many partitions to run-number of input tasks.
      numPartitions = dataGenPartitionsToRun.toInt)
  }

}

object DataGenerator {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf(true)
    conf.setAppName("DataGeneration")
    val sparkSession =
      SparkSession.builder.enableHiveSupport().config(conf).getOrCreate()


    val optionParser = PerfSuiteOptionsParser()
    val argValues = optionParser.parseOptions(args)

    DataGenerator(sparkSession,
      argValues.rootDir,
      argValues.format,
      argValues.scaleFactor,
      argValues.dataGenToolkitPath,
      argValues.dataGenPartitionsToRun,
      argValues.perfSuiteType).generate()

  }


  def apply(sparkSession: SparkSession,
            rootDir: String,
            format: String,
            scaleFactor: String,
            dataGenToolkitPath: Option[String],
            dataGenPartitionsToRun: String,
            perfSuiteType: String): DataGenerator = {

    DataGeneratorFactory().get(sparkSession,
      rootDir,
      format,
      scaleFactor,
      dataGenToolkitPath,
      dataGenPartitionsToRun,
      PerfSuiteType.getPerfSuiteType(perfSuiteType.toUpperCase(Locale.ROOT)))
  }
}

