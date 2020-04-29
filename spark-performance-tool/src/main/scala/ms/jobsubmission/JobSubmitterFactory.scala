package ms.jobsubmission

import ms.jobsubmission.hdi.HdiJobSubmitter
import ms.jobsubmission.hdi.HdiParsedOptions
import ms.jobsubmission.synapse.SynapseJobSubmitter
import ms.jobsubmission.synapse.SynapseParsedOptions


class JobSubmitterFactory() {

  def get(parsedOptions: ParsedOptions): JobSubmitter = {
    parsedOptions match {
      case parsedOptions: HdiParsedOptions => HdiJobSubmitter(parsedOptions)
      case parsedOptions: SynapseParsedOptions => SynapseJobSubmitter(parsedOptions)
      case _ => throw new RuntimeException("Invalid Job Submitter.")
    }
  }
}

object JobSubmitterFactory {
  def apply(): JobSubmitterFactory = new JobSubmitterFactory()
}
