package ms.jobsubmission.synapse

import ms.config
import ms.jobsubmission.OptionsParser
import ms.jobsubmission.ParsedOptions
import ms.jobsubmission.RunnerOptions
import ms.util.PerfSuiteDataGeneratorOptions
import ms.util.PerfSuiteOptionsParser

/**
 * Synapse Options Parser.
 *
 * @param perfSuiteOptionsParser Perf Suite option parser
 */
class SynapseOptionsParser(perfSuiteOptionsParser: PerfSuiteOptionsParser)
  extends OptionsParser {

  override def parse(pathToOptionsJson: String): SynapseParsedOptions = {
    var runnerOptions = parseJson[SynapseRunnerOptions](pathToOptionsJson)
    if (runnerOptions.jarFilePath == null) {
      runnerOptions =
        runnerOptions.copy(jarFilePath = config.defaultJarFilePath)
    }
    SynapseParsedOptions(
      runnerOptions.jarFilePath,
      perfSuiteOptionsParser.parseOptions(runnerOptions.applicationArgs),
      runnerOptions.conf,
      runnerOptions.synapseUri,
      runnerOptions.synapseSparkPool,
      runnerOptions.tenantId,
      runnerOptions.servicePrincipalId,
      runnerOptions.servicePrincipalSecret
    )
  }
}

object SynapseOptionsParser {
  def apply(): SynapseOptionsParser =
    new SynapseOptionsParser(PerfSuiteOptionsParser())
}

private[jobsubmission] case class SynapseParsedOptions(jarFilePath: String,
                                                     applicationArgs: PerfSuiteDataGeneratorOptions,
                                                     conf: Map[String, String],
                                                     synapseUri: String,
                                                     synapseSparkPool: String,
                                                     tenantId: String,
                                                     servicePrincipalId: String,
                                                     servicePrincipalSecret: String
                                                    ) extends ParsedOptions

private[jobsubmission] case class SynapseRunnerOptions(jarFilePath: String,
                                                       applicationArgs: Array[String],
                                                       conf: Map[String, String],
                                                       synapseUri: String,
                                                       synapseSparkPool: String,
                                                       tenantId: String,
                                                       servicePrincipalId: String,
                                                       servicePrincipalSecret: String
                                                      ) extends RunnerOptions
