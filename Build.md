
### Installation process:

Step 1: You can clone and build the repo.

```
git clone https://msdata.visualstudio.com/DefaultCollection/A365/_git/spark-performance-toolkit
mvn clean install
```

Step 2: You can upload the new jars in your own storage location.

Step 3: Provide `jarFilePath` in conf file.

### Running

You can follow [Running spark-performance-tool](./README.md#Running-spark-performance-tool)


    
###Software dependencies

 For data generation and setup, `spark-sql-perf`(https://github.com/databricks/spark-sql-perf) has been added as local dependency in mvn.
 dataGen (dsdgen for tpcds) executable is present in the resources under `datagen-kit-tools` folder. This has been built following instructions here (https://github.com/databricks/tpcds-kit)
