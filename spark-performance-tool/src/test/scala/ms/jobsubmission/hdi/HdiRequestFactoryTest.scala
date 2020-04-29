package ms.jobsubmission.hdi

import java.io.File

import ms.jobsubmission.Job
import ms.util.PerfSuiteOptionsParser
import org.scalatest.FunSpec

class HdiRequestFactoryTest extends FunSpec {

  val perfSuiteOptionsParser = PerfSuiteOptionsParser()
  val expectedOutput = scala.io.Source.fromFile(new File(getClass.getClassLoader.getResource(
    "hdi/sample-hdi-request").getFile).getAbsolutePath).mkString
  val inputArgsFile = new File(getClass.getClassLoader.getResource(
    "synapse/sample-conf").getFile).getAbsolutePath
  val expectedOutputNoJarFilePath = scala.io.Source.fromFile(new File(getClass.getClassLoader.getResource(
    "hdi/sample-hdi-request-noJarPath").getFile).getAbsolutePath).mkString
  val inputArgsFileNoJarFilePath = new File(getClass.getClassLoader.getResource(
    "hdi/sample-conf-noJarPath").getFile).getAbsolutePath

  describe("Test Hdi Request Factory") {
    it("The output json must be as expected") {
      val hdiOptions = HdiOptionsParser().parse(inputArgsFile)
      val job = Job("jobName",
        "test.MainClass",
        hdiOptions.jarFilePath,
        perfSuiteOptionsParser.deserializeParsedOptions(hdiOptions.applicationArgs).split(" "),
        hdiOptions.conf)
      val actualOutput = HdiRequestBodyFactory().getHdiRequest(job)
      assert(actualOutput == expectedOutput)
    }
  }

  describe("Test Hdi Request Factory without jarFilePath") {
    it("It should take the default public storage jarfilepath") {
      val livyOptions = HdiOptionsParser().parse(inputArgsFileNoJarFilePath)
      val job = Job("jobName",
        "test.MainClass",
        livyOptions.jarFilePath,
        perfSuiteOptionsParser.deserializeParsedOptions(livyOptions.applicationArgs).split(" "),
        livyOptions.conf)
      val actualOutputNoJarPath = HdiRequestBodyFactory().getHdiRequest(job)
      assert( actualOutputNoJarPath == expectedOutputNoJarFilePath)
    }
  }

}
