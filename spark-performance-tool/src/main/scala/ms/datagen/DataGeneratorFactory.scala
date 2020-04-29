package ms.datagen

import org.apache.spark.sql.SparkSession

import ms.common.PerfSuiteType
import ms.common.PerfSuiteType.PerfSuiteType
import ms.datagen.tpcds.TPCDSDataGenerator

class DataGeneratorFactory {

  def get(sparkSession: SparkSession,
          rootDir: String,
          format: String,
          scaleFactor: String,
          dataGenToolkitPath: Option[String],
          dataGenPartitionsToRun: String,
          perfSuiteType: PerfSuiteType): DataGenerator = {

    perfSuiteType match {
      case PerfSuiteType.TPCDS | PerfSuiteType.IBMTPCDS =>
        new TPCDSDataGenerator(sparkSession,
          rootDir,
          format,
          scaleFactor,
          dataGenToolkitPath,
          dataGenPartitionsToRun)
      case PerfSuiteType.UNDEFINED => throw new RuntimeException("Invalid Perf Suite Runner.")
    }

  }
}

object DataGeneratorFactory {
  def apply(): DataGeneratorFactory = new DataGeneratorFactory()
}
