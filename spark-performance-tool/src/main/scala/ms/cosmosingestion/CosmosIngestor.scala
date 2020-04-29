package ms.cosmosingestion

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import ms.util.PerfSuiteOptionsParser
import com.microsoft.azure.cosmosdb.spark.config.{Config, CosmosDBConfig}
import com.microsoft.azure.cosmosdb.spark.schema._
import scala.io.Source

class CosmosIngestor(sparkSession: SparkSession,
                     databaseName: String,
                     cosmosEndpoint:String,
                     cosmosAccountKey:String,
                     cosmosDatabaseName:String
                    ) {

  def ingestToCosmos() = {
    val collectionNamesPath: String = "tpcds/collections/names"
    val cosmosTPCDSTables = getCollectionNames(collectionNamesPath)
    cosmosTPCDSTables.foreach { table => {
      var df = sparkSession.sql(s"select * from ${databaseName}.${table}")
      ingestSingleCollection(df, table)
      }
    }
  }

  private def getCollectionNames(pathToCollections: String): Array[String] = {
    val bufferedSource = Source.fromInputStream(getClass.getClassLoader.getResourceAsStream(pathToCollections))
    val lines = (for (line <- bufferedSource.getLines()) yield line).toArray
    bufferedSource.close
    lines
  }

  private def ingestSingleCollection(df: DataFrame, table: String): Unit = {
    val config = Config(Map(
      "CosmosDBConfig.Endpoint" -> s"${cosmosEndpoint}",
      "CosmosDBConfig.Masterkey" -> s"${cosmosAccountKey}",
      "CosmosDBConfig.Database" -> s"${cosmosDatabaseName}",
      "CosmosDBConfig.Collection" -> s"${table}"
    ))
    df.write.mode(saveMode = "overwrite").cosmosDB(config)
    }
  }

object CosmosIngestor {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf(true)
    conf.setAppName("CosmosSynapseIngestor")
    val sparkSession = SparkSession.builder.enableHiveSupport().config(conf).getOrCreate()
    val optionParser = PerfSuiteOptionsParser()
    val argValues = optionParser.parseOptions(args)

    CosmosIngestor(sparkSession,
      argValues.databaseName,
      argValues.cosmosEndpoint,
      argValues.cosmosAccountKey,
      argValues.cosmosDatabaseName).ingestToCosmos()
  }

  def apply(sparkSession: SparkSession, databaseName: String, cosmosEndpoint: String, cosmosAccountKey:String, cosmosDatabaseName: String): CosmosIngestor =
    new CosmosIngestor(sparkSession, databaseName, cosmosEndpoint, cosmosAccountKey, cosmosDatabaseName)
}