package ms.jobsubmission.hdi

import ms.validation.LocalValidation

class HdiLocalValidation(skipDataGeneration: Boolean,
                         skipMetastoreCreation: Boolean,
                         skipQueryExecution: Boolean,
                         dataGenPartitionsToRun: String)
        extends LocalValidation(skipDataGeneration, skipMetastoreCreation, skipQueryExecution,
            dataGenPartitionsToRun) {

    override def validate(): Unit = {
        validateFlags()
    }
}
