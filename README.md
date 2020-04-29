# Cosmos DB Spark Performance Toolkit

This is a performance testing framework for Cosmos DB HTAP on Spark platforms such as Synapse, HDI and Databricks. The framework can also be used to automate accuracy tests on remote Spark runtimes where in the expected and actual datasets can be compared.

# Features
1. Data Generator: Generates standard perf benchmarking datasets such as TPC-DH and TPC-H datasets with the given Scale Factor

2. Cosmos DB Ingestor: Ingests the benchmarking datasets to Cosmos DB collections using OLTP Spark Connector

3. Cosmos-Hive View Creator: Creates Hive Views on top of both OLTP and OLAP collections that has the above ingested datasets. The views offer a convenient way to execute queries that involve complex joins across Cosmos DB collections. 

4. OLTP Query Executor: Executes the standard benchmarking queries against OLTP collections using the OLTP Spark Connector. 

5. OLAP Query Executor: Executes the standard benchmarking queries against OLAP collections using the OLAP Spark Connector. 

6. Parquet Query Executor: Executes the standard queries against benchmarking datasets in the Parquet format in Storage using Synapse / Databricks Spark. This can be useful to compare the query executions against the OLAP store. 

7. Metrics Collector: The query execution metrics such as duration are peristed to a Synapse / Databricks table

8. Result dataset Validator: The Actual reultant dataset for all queries are stored in the project itself and is compared against the Expected dataset to mark it with the Success / Failure Execution status. This can posibly used for accuracy tests and other types of tests that needs to be executed on a remote Spark Wrokspace / Cluster.

9. Summary Generator: The execution results are summarized into csv files and peristed in the provided storage location which can be used to generate comparison dashboards.

10. Config Driven: The features and the Application arguments are driven by configs and can be skipped if needed. 

11. Automated Remote Executor from Local Box: The framework can be used to automate spark job submissions to the Synapse workspace or Databricks cluster

12. Easily extendable and customizable: Additional tests and checks can be easily added to the existing jar, which gets deployed to the Spark runtime. 


# Getting Started

1. Get the latest jar from the public storage container (https://cosmosdbperfexperiments.blob.core.windows.net/cosmosperftoolkit/spark-performance-tool-1.0-SNAPSHOT.jar)
2. If customizations are needed, build the jar and upload to the accessible storage. 
3. Create the Config file with the required options. Below is a sample config file used for executions on Synapse Spark.

    {
	"applicationArgs": [
		"--perfSuiteType",
		"TPCDS",
		"--scaleFactor",
		"100",
		"--skipDataGeneration",
		"true",	
		"--cosmosEndpoint",
		"https://synapse-perf-experiments.documents.azure.com:443/",
		"--cosmosAccountKey",
		"xxxxxxxxx",
		"--cosmosRegion",
		"West US 2",
		"--cosmosDatabaseName",
		"tpcds_sf100",
		"--cosmosSynapseDatabaseName",
		"tpcds_cosmos_sf100",
		"--skipCosmosIngestion",
		"true",
		"--skipCosmosSynapseViewCreation",
		"false",
		"--skipCosmosOLTPQueryExecution",
		"true",
		"--skipCosmosOLAPQueryExecution",
		"false"	
		"--queriesToRun",
		"query1,query2",
		"--runsPerQuery",
		"2",	
		"--format",
		"parquet",	
		"--rootDir",
		"abfs://cosmosdb@cosmosdbperfexperiments.dfs.core.windows.net/data/",
		"--databaseName",
		"tpcds_cosmosdb"],
	"conf": {"spark.executor.instances": "11",
		  "spark.driver.memory": "20g",
		  "spark.executor.memory": "20g",
		  "spark.executor.cores": "8",
		  "spark.driver.cores": "8",
		  "spark.sql.broadcastTimeout": "-1"},
	"synapseUri": "https://cosmosdbperfexperiments.dev.azuresynapse.net",
	"synapseSparkPool": "SPLarge",
	"servicePrincipalId": "15b772a1-a182-4e3b-bd7e-c28854a76f8b",
	"servicePrincipalSecret": "xxxxxxxxx",
	"tenantId": "72f988bf-86f1-41af-91ab-2d7cd011db47",
	"jarFilePath": "https://cosmosdbperfexperiments.blob.core.windows.net/cosmosperftoolkit/spark-performance-tool-1.0-SNAPSHOT.jar"
}

4. Create a Service Principal and add it as Admin on the Syanpse workspace. This is a manual process using Postman as of now and Synapse team is working on making it available on Syanpse WorkSpace. Synapse team is also working on a fix for SP integration for Synapse-Storage interactions. The user token can be used as a work around. Please reach out if more info is needed on the workaround.  

5. Add this service principal as Storage Blob Data Contributor to the default storage account on Synapse workspace. 

6. Below is a sample execution which starts a java process and submits spark job to the workspace / cluster.
   scala -classpath local/path/to/jar/spark-performance-tool-1.0-SNAPSHOT.jar ms.runners.PerformanceSuiteRunner /path/to/conf/file synapse



 