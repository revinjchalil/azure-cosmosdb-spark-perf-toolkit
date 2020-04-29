package ms.jobsubmission.hdi

import java.io.File

import org.scalatest.FunSpec

class HdiLocalValidationTest extends FunSpec {
  val inputArgsFile = new File(getClass.getClassLoader.getResource(
    "synapse/sample-conf").getFile).getAbsolutePath

  val parsedOptions = HdiOptionsParser().parse(inputArgsFile)

  val hdiLocalValidation1 = new HdiLocalValidation(false, true, false, "10")

  val hdiLocalValidation2 = new HdiLocalValidation(false, true, false, "0")

  describe("Test the LivyLocalValidation") {
    it("Test Metastore setup when data generation and query execution is enabled") {
      intercept[IllegalArgumentException] {
        hdiLocalValidation1.validate()
      }
    }

    it("Test dataGenPartitionsToRun when data generation is enabled") {
      intercept[IllegalArgumentException] {
        hdiLocalValidation2.validate()
      }
    }
  }
}
