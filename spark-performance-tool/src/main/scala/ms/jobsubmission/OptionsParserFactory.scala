package ms.jobsubmission

import ms.jobsubmission.hdi.HdiOptionsParser
import ms.jobsubmission.synapse.SynapseOptionsParser

class OptionsParserFactory() {

  def get(jobType: JobSubmitter.JobSubmitterType): OptionsParser = {
    jobType match {
      case JobSubmitter.LIVY => HdiOptionsParser()
      case JobSubmitter.SYNAPSE => SynapseOptionsParser()
      case _ => throw new RuntimeException("Invalid Job Submitter.")
    }
  }
}

object OptionsParserFactory {
  def apply(): OptionsParserFactory = new OptionsParserFactory()
}


