package ms.jobsubmission

import java.io.File

import ms.jobsubmission.hdi.HdiJobSubmitter
import ms.jobsubmission.hdi.HdiOptionsParser
import org.scalatest.FunSpec

class JobSubmitterFactoryTest extends FunSpec {
  val inputArgsFile = new File(getClass.getClassLoader.getResource(
      "synapse/sample-conf").getFile).getAbsolutePath
  val parsedOptions = HdiOptionsParser().parse(inputArgsFile)

  describe("Test Job Submitter Factory") {
    it("The output Job Submitter type must be as expected") {
      val actualOutput = JobSubmitterFactory().get(parsedOptions)
      assert(actualOutput.isInstanceOf[HdiJobSubmitter])
    }
  }

}
