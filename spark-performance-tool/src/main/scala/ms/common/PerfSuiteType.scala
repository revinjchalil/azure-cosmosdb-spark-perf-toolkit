package ms.common

import java.util.Locale

object PerfSuiteType extends Enumeration {

  type PerfSuiteType = Value
  val TPCDS = Value("TPCDS")
  val IBMTPCDS = Value("IBMTPCDS")
  val UNDEFINED = Value("UNDEFINED")

  def getPerfSuiteType(s: String): PerfSuiteType = {
    val perfSuiteTypeStr = s.toUpperCase(Locale.ROOT)
    if (values.exists(_.toString == perfSuiteTypeStr)) {
      PerfSuiteType withName perfSuiteTypeStr
    } else {
      PerfSuiteType.UNDEFINED
    }
  }
}
