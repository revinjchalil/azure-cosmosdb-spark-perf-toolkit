package ms.jobsubmission.synapse

import ms.validation.LocalValidation

class SynapseLocalValidation(skipDataGeneration: Boolean,
                             skipMetastoreCreation: Boolean,
                             skipQueryExecution: Boolean,
                             dataGenPartitionsToRun: String,
                             conf: Map[String, String])
        extends LocalValidation(skipDataGeneration, skipMetastoreCreation, skipQueryExecution,
          dataGenPartitionsToRun) {

    override def validate(): Unit = {
      validateFlags()
      validateConf()
    }

  private def validateConf(): Unit = {
    var validation: Boolean = false
    if (conf != null) {
      val keySet = conf.keySet
      if (keySet.contains("spark.executor.instances") &&
          keySet.contains("spark.driver.memory") &&
          keySet.contains("spark.executor.memory") &&
          keySet.contains("spark.executor.cores") &&
          keySet.contains("spark.driver.cores")) {
        validation = true
      }
    }
    if (!validation) {
      throw new IllegalArgumentException(
        "Please provide spark conf values. It is mandatory to provide values for " +
          "\"spark.executor.instances\", \"spark.driver.memory\", " +
          "\"spark.executor.memory\", \"spark.executor.cores\", \"spark.driver.cores\"."
      )
    }
  }
}
