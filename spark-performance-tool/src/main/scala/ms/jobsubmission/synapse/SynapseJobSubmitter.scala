package ms.jobsubmission.synapse

import java.net.HttpURLConnection
import java.util.concurrent.TimeUnit

import ms.config
import ms.jobsubmission.Job
import ms.jobsubmission.JobStatus.JobStatus
import ms.jobsubmission.JobSubmitter
import ms.jobsubmission.util.AADTokenUtil
import ms.jobsubmission.util.LivyUtil
import ms.util.PerfSuiteDataGeneratorOptions

class SynapseJobSubmitter(synapseEndpointUri: String,
                          synapseSparkPool: String,
                          tenantId: String,
                          servicePrincipalId: String,
                          servicePrincipalSecret: String,
                          synapseRequestBodyFactory: SynapseRequestBodyFactory)
  extends JobSubmitter {

  val url = synapseEndpointUri + SynapseJobSubmitter.SynapseBatchAPI.format(synapseSparkPool)
  val livyUtil = LivyUtil()
  val aaDTokenUtil = new AADTokenUtil(tenantId, servicePrincipalId, servicePrincipalSecret)

  override def runJob(job: Job): JobStatus = {
    val requestBody = synapseRequestBodyFactory.getSynapseRequest(job)
    val sessionId = livyUtil.submitJob(url, getAuthHeader(), requestBody,
      HttpURLConnection.HTTP_OK, config.livyRetryCount)
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
    new SynapseLocalValidation(applicationArgs.skipDataGeneration,
      applicationArgs.skipMetastoreCreation,
      applicationArgs.skipQueryExecution,
      applicationArgs.dataGenPartitionsToRun,
      conf).validate()
  }



  /**
   * @return AAD auth token.
   */
  private def getAuthHeader(): String = {
    val token = aaDTokenUtil.acquireToken()
//    val authHeaderValue: String = "Bearer " + new String(token)
    val authHeaderValue: String = "Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6IkN0VHVoTUptRDVNN0RMZHpEMnYyeDNRS1NSWSIsImtpZCI6IkN0VHVoTUptRDVNN0RMZHpEMnYyeDNRS1NSWSJ9.eyJhdWQiOiJodHRwczovL2Rldi5henVyZXN5bmFwc2UubmV0IiwiaXNzIjoiaHR0cHM6Ly9zdHMud2luZG93cy5uZXQvNzJmOTg4YmYtODZmMS00MWFmLTkxYWItMmQ3Y2QwMTFkYjQ3LyIsImlhdCI6MTU4ODEzMzA1OCwibmJmIjoxNTg4MTMzMDU4LCJleHAiOjE1ODgxMzY5NTgsImFjciI6IjEiLCJhaW8iOiJBVlFBcS84UEFBQUFQaXpTMXloSHhwVTdmQmFvQnd2aXBMZks3dFpZK09TYzlnUlFuRXEwekZYSXhjbXlOeEJVYmNTWW9rWlZLK0ZpNEEzQzRyNTVtQUc3c0p2dGxaLzNBUWRWV3UvbTBmNHZrcERMQ2RzQ2h5RT0iLCJhbXIiOlsicHdkIiwibWZhIl0sImFwcGlkIjoiZWM1MmQxM2QtMmU4NS00MTBlLWE4OWEtOGM3OWZiNmEzMmFjIiwiYXBwaWRhY3IiOiIwIiwiZGV2aWNlaWQiOiIxMzhkMDY2MS1mN2RlLTQxNDgtYjNlZC05Y2FjMGRmYTQ0NjIiLCJmYW1pbHlfbmFtZSI6IkNoYWxpbCIsImdpdmVuX25hbWUiOiJSZXZpbiIsImhhc2dyb3VwcyI6InRydWUiLCJpcGFkZHIiOiIyNC4xNy4yMDQuMjUxIiwibmFtZSI6IlJldmluIENoYWxpbCIsIm9pZCI6ImRjMTk5NmQ2LTE3ZDktNGU1Zi1iNWNkLWJmOWYyNzQ3ZWMwOCIsIm9ucHJlbV9zaWQiOiJTLTEtNS0yMS0yMTI3NTIxMTg0LTE2MDQwMTI5MjAtMTg4NzkyNzUyNy0zMTAxNzc4NiIsInB1aWQiOiIxMDAzM0ZGRkE5NDQ4Nzk5IiwicmgiOiIwLkFSb0F2NGo1Y3ZHR3IwR1JxeTE4MEJIYlJ6M1JVdXlGTGc1QnFKcU1lZnRxTXF3YUFJQS4iLCJzY3AiOiJ3b3Jrc3BhY2VhcnRpZmFjdHMubWFuYWdlbWVudCIsInN1YiI6IjE3MVBFXzhQdzB1aDBEMHdrSmZ2ZFkzMWpnUUpvbXF1eEprMDBaSHBQTTQiLCJ0aWQiOiI3MmY5ODhiZi04NmYxLTQxYWYtOTFhYi0yZDdjZDAxMWRiNDciLCJ1bmlxdWVfbmFtZSI6InJlY2hhbGlsQG1pY3Jvc29mdC5jb20iLCJ1cG4iOiJyZWNoYWxpbEBtaWNyb3NvZnQuY29tIiwidXRpIjoicVM4aU5WSkZEa3lpYlJCZEdOMEZBQSIsInZlciI6IjEuMCJ9.sYR8oH8KtfM7dUjMtw_OBzIxoCyF3T-5sGT7pNfZIkqbuf1KRDqjzWsTT7zaRo2VrFGg6HVEUcp6cWxo5aCmbNmQZ4kp34Hbt6hI--9vNR7hF03NAnlXQph-3BnON_-m4XhkhVEqr5uOE0Ans_DnBXjjtHQAWUrmE75F3dSMiqs42Jwm5hD5YAsgXfzNEE2kTTKzyLVipS7I0uXioHSNqT74T4moFSErCbPPktnpTGr-yggodABaFUffG7AtAmNDOH0N9SnEG5rRaRJEPoujtXEr1j1Y1Mdb5tZRrMsVffBXmfImO3Fifp3eYC2sR6hiECJB8b0mxWHUQ9EgzvDYwQ"
    authHeaderValue
  }

  /**
   * Close running threads
   */
  override def close(): Unit = {
    aaDTokenUtil.close()
  }
}

object SynapseJobSubmitter {

  val SynapseBatchAPI = "/livyApi/sparkpools/%s/batches"

  def apply(parsedOptions: SynapseParsedOptions): SynapseJobSubmitter = {
    new SynapseJobSubmitter(parsedOptions.synapseUri, parsedOptions.synapseSparkPool,
      parsedOptions.tenantId, parsedOptions.servicePrincipalId,
      parsedOptions.servicePrincipalSecret, SynapseRequestBodyFactory())
  }
}