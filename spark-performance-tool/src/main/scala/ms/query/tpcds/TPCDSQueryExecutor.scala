package ms.query.tpcds

import org.apache.spark.sql.SparkSession

import ms.query.QueryExecutor
import ms.query.QueryResultValidator
import ms.util.FSUtil

/**
 * QueryExecutor class to run the TPCDS sql queries, records the metrics and writes to
 * the outputMetricsPath in json format.
 *
 * @param sparkSession spark session.
 * @param databaseName database name, on which the queries will be executed.
 * @param queryNames   query names, comma separated.
 * @param outputPath   path where the output metrics will be written in json format.
 * @param runsPerQuery number of times each TPCDS query should be run.
 */
class TPCDSQueryExecutor(sparkSession: SparkSession,
                         databaseName: String,
                         scaleFactor: String,
                         queryNames: String,
                         outputPath: String,
                         runsPerQuery: Int,
                         fsUtil: FSUtil) extends
  QueryExecutor(sparkSession, databaseName, queryNames, outputPath, runsPerQuery, fsUtil) {
  override def queryBasePath: String = TPCDSQueryExecutor.tpcdsQueriesPathInResources

  override def queryResultValidator: QueryResultValidator =
    QueryResultValidator(s"tpcds/validation/tpcds-${scaleFactor}")
}

object TPCDSQueryExecutor {
  val tpcdsQueriesPathInResources = "tpcds/queries"
}

