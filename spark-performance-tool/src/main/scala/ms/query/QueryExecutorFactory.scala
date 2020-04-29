package ms.query

import org.apache.spark.sql.SparkSession

import ms.common.PerfSuiteType
import ms.common.PerfSuiteType.PerfSuiteType
import ms.query.ibmtpcds.IBMTpcdsQueryExecutor
import ms.query.tpcds.TPCDSQueryExecutor
import ms.util.FSUtil

class QueryExecutorFactory {

  def get(sparkSession: SparkSession,
          databaseName: String,
          scaleFactor: String,
          queryNames: String,
          outputPath: String,
          runsPerQuery: Int,
          fsUtil: FSUtil,
          perfSuiteType: PerfSuiteType): QueryExecutor = {

    perfSuiteType match {
      case PerfSuiteType.TPCDS => new TPCDSQueryExecutor(sparkSession,
        databaseName,
        scaleFactor,
        queryNames,
        outputPath,
        runsPerQuery,
        fsUtil)
      case PerfSuiteType.IBMTPCDS => new IBMTpcdsQueryExecutor(sparkSession,
        databaseName,
        scaleFactor,
        queryNames,
        outputPath,
        runsPerQuery,
        fsUtil)
      case PerfSuiteType.UNDEFINED => throw new RuntimeException("Invalid Perf Suite Runner.")
    }
  }
}

object QueryExecutorFactory {
  def apply(): QueryExecutorFactory = new QueryExecutorFactory()
}
