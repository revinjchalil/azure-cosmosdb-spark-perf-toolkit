package ms.util.tpcds

import com.databricks.spark.sql.perf.tpcds.DSDGEN
import com.databricks.spark.sql.perf.tpcds.TPCDSTables
import org.apache.spark.sql.SQLContext

/**
 * TPCDS tables wrapper to hook in our version of DSDGEN.
 * @param sqlContext
 * @param dsdgenDir
 * @param scaleFactor
 * @param useDoubleForDecimal
 * @param useStringForDate
 */
class TPCDSTablesWrapper(sqlContext: SQLContext,
                         dsdgenDir: String,
                         scaleFactor: String,
                         useDoubleForDecimal: Boolean = false,
                         useStringForDate: Boolean = false) extends TPCDSTables(
  sqlContext, dsdgenDir, scaleFactor, useDoubleForDecimal, useStringForDate) {
  override val dataGenerator: DSDGEN = new DSDGENWrapper(dsdgenDir)
}
