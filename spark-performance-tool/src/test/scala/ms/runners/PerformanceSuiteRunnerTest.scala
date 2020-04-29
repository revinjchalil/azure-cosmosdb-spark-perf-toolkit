package ms.runners

import java.io.File

import ms.jobsubmission.Job
import ms.jobsubmission.JobStatus
import ms.jobsubmission.JobSubmitter
import ms.jobsubmission.OptionsParserFactory
import ms.util.PerfSuiteOptionsParser
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.FunSpec
import org.scalatest.MustMatchers
import org.scalatestplus.mockito.MockitoSugar

class PerformanceSuiteRunnerTest extends FunSpec
  with MustMatchers
  with BeforeAndAfterAll
  with MockitoSugar {

  val inputArgsFile = new File(getClass.getClassLoader.getResource(
    "hdi/sample-conf").getFile).getAbsolutePath

  val options = OptionsParserFactory().get(JobSubmitter.LIVY).parse(inputArgsFile)
  val mockedJobSubmitter = mock[JobSubmitter]
  val perfSuiteOptionsParser = PerfSuiteOptionsParser()
  val perfRunner = new PerformanceSuiteRunner(mockedJobSubmitter, options.jarFilePath,
    options.applicationArgs, options.conf, perfSuiteOptionsParser)
  override def beforeAll(): Unit = {
    when(mockedJobSubmitter.runJob(any[Job])).thenReturn(JobStatus.SUCCEEDED)
  }

  describe("Test Performance Suite Runner") {
    it("Test the job submission") {
      perfRunner.run()
      // total 5 jobs as per current args as follows:
      // validation, pre-perf, query2, query3, query5, summaryGeneration
      verify(mockedJobSubmitter, times(6)).runJob(any[Job])
    }
  }
}
