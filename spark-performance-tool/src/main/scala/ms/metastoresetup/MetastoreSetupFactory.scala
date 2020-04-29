package ms.metastoresetup

import org.apache.spark.sql.SparkSession

import ms.common.PerfSuiteType
import ms.common.PerfSuiteType.PerfSuiteType
import ms.metastoresetup.tpcds.TPCDSMetastoreSetup

class MetastoreSetupFactory {

  def get(sparkSession: SparkSession,
          rootDir: String,
          databaseName: String,
          format: String,
          scaleFactor: String,
          dataGenToolkitPath: Option[String],
          perfSuiteType: PerfSuiteType): MetastoreSetup = {

    perfSuiteType match {
      case PerfSuiteType.TPCDS | PerfSuiteType.IBMTPCDS =>
        new TPCDSMetastoreSetup(sparkSession,
          rootDir,
          databaseName,
          format,
          scaleFactor,
          dataGenToolkitPath)
      case PerfSuiteType.UNDEFINED => throw new RuntimeException("Invalid Perf Suite Runner.")
    }

  }
}

object MetastoreSetupFactory {
  def apply(): MetastoreSetupFactory = new MetastoreSetupFactory()
}


