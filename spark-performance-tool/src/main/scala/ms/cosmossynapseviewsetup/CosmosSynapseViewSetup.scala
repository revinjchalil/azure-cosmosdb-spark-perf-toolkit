package ms.cosmossynapseviewsetup

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import ms.util.PerfSuiteOptionsParser
import scala.io.Source

class CosmosSynapseViewSetup(sparkSession: SparkSession,
                                      cosmosEndpoint:String,
                                      cosmosAccountKey:String,
                                      cosmosRegion: String,
                                      cosmosDatabaseName:String,
                                      cosmosSynapseDatabaseName: String
                                      ) {

  def createCosmosHiveViews() = {
    createHiveViewDBs()
    val collectionNamesPath: String = "tpcds/collections/names"
    val cosmosTPCDSTables = getCollectionNames(collectionNamesPath)
    createViews(cosmosTPCDSTables)
    createMetricsCollTable
  }

  private def createHiveViewDBs(): Unit = {
    sparkSession.sql(s"drop database if exists ${cosmosSynapseDatabaseName}_OLTP cascade")
    sparkSession.sql(s"create database ${cosmosSynapseDatabaseName}_OLTP")
    sparkSession.sql(s"drop database if exists ${cosmosSynapseDatabaseName}_OLAP cascade")
    sparkSession.sql(s"create database ${cosmosSynapseDatabaseName}_OLAP")
  }

  private def getCollectionNames(pathToCollections: String): Array[String] = {
    val bufferedSource = Source.fromInputStream(getClass.getClassLoader.getResourceAsStream(pathToCollections))
    val lines = (for (line <- bufferedSource.getLines()) yield line).toArray
    bufferedSource.close
    lines
  }

  private def createViews(cosmosTPCDSTables: Array[String]): Unit = {
    cosmosTPCDSTables.foreach { table => {
      sparkSession.sql(s"use ${cosmosSynapseDatabaseName}_OLTP")
      var viewDDLOLTP = s"""create table if not exists ${table}
                            using com.microsoft.azure.cosmosdb.spark
                            options(endpoint '${cosmosEndpoint}'
                                  , masterkey '${cosmosAccountKey}'
                                  , database '${cosmosDatabaseName}'
                                  , collection '${table}')"""
      sparkSession.sql(viewDDLOLTP)

      sparkSession.sql(s"use ${cosmosSynapseDatabaseName}_OLAP")
      var viewDDLOLAP = s"""create table if not exists ${table}
                            using cosmos.olap
                            options(spark.cosmos.accountEndpoint '${cosmosEndpoint}'
                                  , spark.cosmos.accountKey '${cosmosAccountKey}'
                                  , spark.cosmos.region '${cosmosRegion}'
                                  , spark.cosmos.database '${cosmosDatabaseName}'
                                  , spark.cosmos.container '${table}')"""
      sparkSession.sql(viewDDLOLAP)
    }
    }
  }

  private def createMetricsCollTable(): Unit = {
    sparkSession.sql(s"drop database if exists tpcds_metrics cascade")
    sparkSession.sql(s"create database tpcds_metrics")
    sparkSession.sql(s"""create table tpcds_metrics.cosmos_query_metrics(
                                          query_type String,
                                          query_name String,
                                          run_id String,
                                          start_time String,
                                          end_time String,
                                          query_result_validation_status String,
                                          executaion_duration_millisec String,
                                          executaion_duration_minutes String)""")
    }
}

object CosmosSynapseViewSetup {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf(true)
    conf.setAppName("CosmosSynapseViewSetup")
    val sparkSession = SparkSession.builder.enableHiveSupport().config(conf).getOrCreate()
    val optionParser = PerfSuiteOptionsParser()
    val argValues = optionParser.parseOptions(args)

    CosmosSynapseViewSetup(sparkSession,
      argValues.cosmosEndpoint,
      argValues.cosmosAccountKey,
      argValues.cosmosRegion,
      argValues.cosmosDatabaseName,
      argValues.cosmosSynapseDatabaseName).createCosmosHiveViews()
     }

  def apply(sparkSession: SparkSession, cosmosEndpoint: String, cosmosAccountKey:String, cosmosRegion: String, cosmosDatabaseName: String, cosmosSynapseDatabaseName: String): CosmosSynapseViewSetup =
    new CosmosSynapseViewSetup(sparkSession, cosmosEndpoint, cosmosAccountKey, cosmosRegion, cosmosDatabaseName, cosmosSynapseDatabaseName)
}



