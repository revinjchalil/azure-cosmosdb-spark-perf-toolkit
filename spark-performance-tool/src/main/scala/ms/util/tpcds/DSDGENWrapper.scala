package ms.util.tpcds

import com.databricks.spark.sql.perf.BlockingLineStream
import com.databricks.spark.sql.perf.tpcds.DSDGEN
import org.apache.spark.{SparkContext, SparkFiles}

/**
 * DSDGEN wrapper to generate TPCDS data.
 * @param dsdgenDir path where DSDGEN has been placed.
 */

class DSDGENWrapper(dsdgenDir: String) extends DSDGEN(dsdgenDir) {

  // scalastyle:off
  override def generate(sparkContext: SparkContext,
                        name: String, partitions: Int,
                        scaleFactor: String) = {
    val generatedData = {
      sparkContext.parallelize(1 to partitions, partitions).flatMap { i =>
        val dsdgenDir = SparkFiles.getRootDirectory()
        val dsdgen = s"$dsdgenDir/dsdgen"
        val localToolsDir = if (new java.io.File(dsdgen).exists) {
          dsdgenDir
        } else if (new java.io.File(s"/$dsdgen").exists) {
          s"/$dsdgenDir"
        } else {
          sys.error(s"Could not find dsdgen at $dsdgen or /$dsdgen. Run install")
        }

        // Note: RNGSEED is the RNG seed used by the data generator. Right now, it is fixed to 100.
        val parallel = if (partitions > 1) s"-parallel $partitions -child $i" else ""
        val commands = Seq(
          "bash", "-c",
          s"cd $localToolsDir/ && ./dsdgen -table $name -filter Y -scale $scaleFactor" +
            s" -RNGSEED 100 $parallel")
        // scalastyle:off println
        println(commands)
        // scalastyle:on println
        BlockingLineStream(commands)
      }
    }
    generatedData.setName(s"$name, sf=$scaleFactor, strings")
    generatedData
  }
  // scalastyle:on
}
