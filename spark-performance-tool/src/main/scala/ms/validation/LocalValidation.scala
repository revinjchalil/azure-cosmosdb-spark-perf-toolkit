package ms.validation

/**
 *  To validate each run before moving ahead to run it.
 */
abstract class LocalValidation(skipDataGeneration: Boolean,
                      skipMetastoreCreation: Boolean,
                      skipQueryExecution: Boolean,
                      dataGenPartitionsToRun: String) {

  def validate(): Unit

  /**
   * This function validates flag values.
   */
  def validateFlags(): Unit = {
    if (!skipDataGeneration && skipMetastoreCreation && !skipQueryExecution) {
      throw new IllegalArgumentException(
        "Invalid value for skipMetastoreCreation. " +
          "Can't execute queries without Metastore."
      )
    }

    if (!skipDataGeneration && dataGenPartitionsToRun.toInt == 0) {
      throw new IllegalArgumentException(
        "Invalid value for dataGenPartitionsToRun. " +
          "dataGenPartitionsToRun take positive integer value."
      )
    }

  }
}

