package ms

import org.apache.spark.sql.SparkSession
import org.scalatest.FunSpec

trait LocalSparkSession extends FunSpec {
    lazy val spark: SparkSession = {
      SparkSession
        .builder()
        .master("local")
        .appName("spark_test")
        .getOrCreate()
    }

  def beforeAll(): Unit = {
    spark.sparkContext.setLogLevel("WARN")
  }

}
