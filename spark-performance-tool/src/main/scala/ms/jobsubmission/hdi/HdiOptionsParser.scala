package ms.jobsubmission.hdi

import ms.config
import ms.jobsubmission.OptionsParser
import ms.jobsubmission.ParsedOptions
import ms.jobsubmission.RunnerOptions
import ms.util.{PerfSuiteDataGeneratorOptions, PerfSuiteOptionsParser}

/**
 * Livy Parser.
 */
class HdiOptionsParser(perfSuiteOptionsParser: PerfSuiteOptionsParser) extends OptionsParser {

  override def parse(pathToOptionsJson: String): HdiParsedOptions = {
    var runnerOptions = parseJson[HdiRunnerOptions](pathToOptionsJson)
    if (runnerOptions.jarFilePath == null) {
      runnerOptions = runnerOptions.copy(jarFilePath = config.defaultJarFilePath)
    }
    HdiParsedOptions(
      runnerOptions.jarFilePath,
      perfSuiteOptionsParser.parseOptions(runnerOptions.applicationArgs),
      runnerOptions.conf,
      runnerOptions.livyUri,
      runnerOptions.userName,
      runnerOptions.userPassword
    )
  }
}

object HdiOptionsParser {
  def apply(): HdiOptionsParser
  = new HdiOptionsParser(PerfSuiteOptionsParser())
}

private[jobsubmission] case class HdiParsedOptions(jarFilePath: String,
                                                   applicationArgs: PerfSuiteDataGeneratorOptions,
                                                   conf: Map[String, String],
                                                   livyUri: String,
                                                   userName: String,
                                                   userPassword: String) extends ParsedOptions

private[jobsubmission] case class HdiRunnerOptions(jarFilePath: String,
                                                   applicationArgs: Array[String],
                                                   conf: Map[String, String],
                                                   livyUri: String,
                                                   userName: String,
                                                   userPassword: String) extends RunnerOptions

