package ms.jobsubmission

import ms.jobsubmission.hdi.HdiOptionsParser
import org.scalatest.FunSpec

class OptionsParserFactoryTest extends FunSpec {

  describe("Test Options Parser Factory") {
    it("The output Options Parser type must be as expected") {
      val actualOutput = OptionsParserFactory().get(JobSubmitter.LIVY)
      assert(actualOutput.isInstanceOf[HdiOptionsParser])
    }
  }
}
