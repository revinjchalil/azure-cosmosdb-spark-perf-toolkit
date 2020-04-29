package ms.jobsubmission.hdi

import java.net.HttpURLConnection
import java.nio.charset.StandardCharsets
import java.util.concurrent.TimeUnit

import org.apache.commons.codec.binary.Base64

import ms.config
import ms.jobsubmission.Job
import ms.jobsubmission.JobStatus.JobStatus
import ms.jobsubmission.JobSubmitter
import ms.jobsubmission.util.LivyUtil
import ms.util.PerfSuiteDataGeneratorOptions

class HdiJobSubmitter(hdiEndpointUri: String,
                      userName: String,
                      password: String,
                      hdiRequestBodyFactory: HdiRequestBodyFactory) extends JobSubmitter{

  val url = hdiEndpointUri + HdiJobSubmitter.livyBatchAPI
  val livyUtil = LivyUtil()

  override def runJob(job: Job): JobStatus = {
    val requestBody = hdiRequestBodyFactory.getHdiRequest(job)
    val sessionId = livyUtil.submitJob(url, getAuthHeader(), requestBody,
      HttpURLConnection.HTTP_CREATED, config.livyRetryCount)
    pollJobCompletion(sessionId)
    val livySessionState = livyUtil.fetchSessionState(url, sessionId, getAuthHeader(),
      config.livyRetryCount)
    livyUtil.getJobStatus(livySessionState)
  }

  def pollJobCompletion(sessionId: Int): Unit = {
    while (livyUtil.checkInProgressJob(url, sessionId, getAuthHeader())) {
      TimeUnit.SECONDS.sleep(config.livyActiveJobPollingIntervalInSeconds)
    }
  }

  override def localValidation(applicationArgs: PerfSuiteDataGeneratorOptions,
                               conf: Map[String, String]): Unit = {
    new HdiLocalValidation(applicationArgs.skipDataGeneration,
      applicationArgs.skipMetastoreCreation,
      applicationArgs.skipQueryExecution,
      applicationArgs.dataGenPartitionsToRun).validate()
  }

  /**
   * @return encoded authentication Header value from username and password.
   */
  private def getAuthHeader(): String = {
    val auth: String = userName + ":" + password
    val encodedAuth: Array[Byte] = Base64.encodeBase64(auth.getBytes(StandardCharsets.UTF_8))
    val authHeaderValue: String = "Basic " + new String(encodedAuth)
    authHeaderValue
  }

  /**
   * Close running threads
   */
  override def close(): Unit = {}
}

object HdiJobSubmitter {

  val livyBatchAPI = "/livy/batches"

  def apply(parsedOptions: HdiParsedOptions): HdiJobSubmitter = {
    new HdiJobSubmitter(parsedOptions.livyUri, parsedOptions.userName, parsedOptions.userPassword,
      HdiRequestBodyFactory())
  }
}
