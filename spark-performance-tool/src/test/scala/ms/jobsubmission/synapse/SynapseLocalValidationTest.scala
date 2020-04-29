package ms.jobsubmission.synapse

import java.io.File

import org.scalatest.FunSpec

class SynapseLocalValidationTest extends FunSpec {
  val inputArgsFile = new File(getClass.getClassLoader.getResource("synapse/sample-conf").getFile
  ).getAbsolutePath

  val parsedOptions = SynapseOptionsParser().parse(inputArgsFile)

  val conf = Map(("spark.sql.broadcastTimeout", "-1"),
                ("spark.executor.instances", "15"),
                ("spark.driver.memory", "28g"),
                ("spark.executor.memory", "28g"),
                ("spark.executor.cores", "4"),
                ("spark.driver.cores", "4"))

  val synapseLocalValidation1 = new SynapseLocalValidation(false, true, false, "10", conf)

  val synapseLocalValidation2 = new SynapseLocalValidation(false, true, false, "0", conf)

  describe("Test the SynapseLocalValidation") {
    it("Test spark conf avail") {
      intercept[IllegalArgumentException] {
        synapseLocalValidation1.validate()
      }
    }
  }
}
