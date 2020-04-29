package ms.jobsubmission.synapse

import java.io.File

import ms.jobsubmission.Job
import ms.util.PerfSuiteOptionsParser
import org.scalatest.FunSpec

class SynapseRequestFactoryTest extends FunSpec {

  val perfSuiteOptionsParser = PerfSuiteOptionsParser()
  val expectedOutput = scala.io.Source.fromFile(new File(getClass.getClassLoader.getResource(
    "synapse/sample-synapse-request").getFile).getAbsolutePath).mkString
  val inputArgsFile = new File(getClass.getClassLoader.getResource(
    "synapse/sample-conf").getFile).getAbsolutePath
  val expectedOutputNoJarFilePath = scala.io.Source.fromFile(new File(getClass.getClassLoader.getResource(
    "synapse/sample-synapse-request-noJarPath").getFile).getAbsolutePath).mkString
  val inputArgsFileNoJarFilePath = new File(getClass.getClassLoader.getResource(
    "synapse/sample-conf-noJarPath").getFile).getAbsolutePath

  describe("Test Synapse Request Factory") {
    it("The output json must be as expected") {
      val synapseOptions = SynapseOptionsParser().parse(inputArgsFile)
      val job = Job("jobName",
        "test.MainClass",
        synapseOptions.jarFilePath,
        perfSuiteOptionsParser.deserializeParsedOptions(synapseOptions.applicationArgs).split(" "),
        synapseOptions.conf)
      val actualOutput = SynapseRequestBodyFactory().getSynapseRequest(job)
      assert(actualOutput == expectedOutput)
    }
  }

  describe("Test Synapse Request Factory without jarFilePath") {
    it("It should take the default public storage jarfilepath") {
      val livyOptions = SynapseOptionsParser().parse(inputArgsFileNoJarFilePath)
      val job = Job("jobName",
        "test.MainClass",
        livyOptions.jarFilePath,
        perfSuiteOptionsParser.deserializeParsedOptions(livyOptions.applicationArgs).split(" "),
        livyOptions.conf)
      val actualOutputNoJarPath = SynapseRequestBodyFactory().getSynapseRequest(job)
      assert( actualOutputNoJarPath == expectedOutputNoJarFilePath)
    }
  }

}
