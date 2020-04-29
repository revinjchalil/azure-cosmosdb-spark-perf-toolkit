package ms.jobsubmission.util

import java.io.BufferedReader
import java.io.InputStreamReader
import java.net.HttpURLConnection
import java.net.URL
import java.util.concurrent.TimeUnit

import com.fasterxml.jackson.databind.ObjectMapper
import com.typesafe.scalalogging.slf4j.LazyLogging

import ms.config
import ms.jobsubmission.JobStatus
import ms.jobsubmission.JobStatus.JobStatus

/**
 * This util class is used by job submitters who submits spark jobs using livy.
 */
class LivyUtil extends LazyLogging {

  /**
   * Submit a livy job
   * @param url livy endpoint
   * @param authHeader Auth Header
   * @param requestBody Request Body
   * @param expectedResponseCode Expected response code from http connection in case of successful
   *                             job submission
   * @param retryCount Number of retries possible
   * @return sessionId for livy session created
   */
  def submitJob(url: String, authHeader: String, requestBody: String,
                expectedResponseCode: Int, retryCount: Int) : Int = {
    logger.info(s"Submitting a Livy batch job to the url: $url\n request: $requestBody")
    val httpURLConnection = createPostConnection(url, authHeader)
    try {
      val outputStream = httpURLConnection.getOutputStream
      outputStream.write(requestBody.getBytes())
      outputStream.flush()
      outputStream.close()
    } catch {
      case exception: Exception => logger.error(
        s"Encountered error while submitting a livy job: ${exception.getStackTrace}")
    }
    val responseCode = httpURLConnection.getResponseCode
    if (responseCode == expectedResponseCode) {
      val sessionId = getLivySessionId(httpURLConnection)
      if (sessionId == -1) {
        throw new RuntimeException("Unexpected livy response for batch job submission")
      }
      logger.info(s"Session Id: $sessionId")
      logger.info("Job Submitted! Waiting for the current livy job to " +
              "finish before proceeding.")
      sessionId
    } else {
      if (retryCount > 0) {
        logger.info("Retrying Job Submission.")
        TimeUnit.SECONDS.sleep(config.livyActiveJobPollingIntervalInSeconds)
        return submitJob(url, authHeader, requestBody, expectedResponseCode, retryCount - 1)
      }
      throw new RuntimeException(
        s"Livy Batch job submission failed with response code: $responseCode")
    }
  }

  /**
   * Get livy session id
   * @param httpURLConnection http connection object
   * @return
   */
  def getLivySessionId(httpURLConnection: HttpURLConnection): Int = {
    var sessionId = -1
    try {
      val mapper = new ObjectMapper
      sessionId =
        mapper.readTree(readResponse(httpURLConnection)).at("/id").asInt()
    } catch {
      case exception: Exception =>
        logger.error("Exception while parsing the livy response", exception)
    }
    sessionId
  }

  /**
   * Read response from an http connection
   * @param httpURLConnection
   * @return Response as a string
   */
  def readResponse(httpURLConnection: HttpURLConnection): String = {
    try {
      val bufferedReader: BufferedReader =
        new BufferedReader(
          new InputStreamReader(httpURLConnection.getInputStream)
        )
      var line = bufferedReader.readLine()
      var output = line
      while (line.isEmpty) {
        output = output + s"\n$line"
        line = bufferedReader.readLine()
      }
      bufferedReader.close()
      logger.debug(s"The response is: $output")
      output
    } catch {
      case exception: Exception =>
        logger.error(
          s"Exception while reading the livy post response: ${exception.getStackTrace}"
        )
        ""
    }
  }

  /**
   * Make a livy call to get the job status.
   *
   * @return true if there are 'running' or 'starting' batch sessions, else false.
   */
  def checkInProgressJob(baseUri: String,
                         sessionId: Int,
                         authHeader: String): Boolean = {
    val state = fetchSessionState(baseUri, sessionId, authHeader, config.livyRetryCount)
    val jobStatus = getJobStatus(state)
    if (jobStatus == JobStatus.RUNNING) {
      true
    } else {
      false
    }
  }

  /**
   * Map livy sesion state to job status enum
   *
   * @param status livy session state
   * @return Job status
   */
  def getJobStatus(status: String): JobStatus = {
    status.toUpperCase() match {
      case "SUCCESS" => JobStatus.SUCCEEDED
      case "ERROR" | "DEAD" | "KILLED" => JobStatus.FAILED
      case _ => JobStatus.RUNNING
    }
  }

  /**
   * Get final session state for given session id
   * @param baseUri base uri for livy
   * @param sessionId
   * @param authHeader
   * @param retryCount Number of retries possible
   * @return
   */
  def fetchSessionState(baseUri: String,
                        sessionId: Int,
                        authHeader: String,
                        retryCount: Int): String = {
    val url = baseUri + s"/$sessionId"
    val httpURLConnection = createGetConnection(url, authHeader)
    val responseCode = httpURLConnection.getResponseCode
    if (responseCode == HttpURLConnection.HTTP_OK) {
      val response = readResponse(httpURLConnection)
      val mapper = new ObjectMapper
      val state = mapper.readTree(response).at("/state").asText()
      state
    } else {
      if (retryCount > 0) {
        logger.info("Retrying livy api to get session state.")
        TimeUnit.SECONDS.sleep(config.livyActiveJobPollingIntervalInSeconds)
        return fetchSessionState(baseUri, sessionId, authHeader, retryCount - 1)
      }
      throw new RuntimeException(
        s"Livy Batch job fetching status of livy session $sessionId failed" +
          s" with response code: $responseCode"
      )
    }
  }

  private def createGetConnection(url: String,
                                  authHeader: String,
                                  retryCount: Int = config.livyRetryCount): HttpURLConnection = {
    try {
      httpGetConnection(url, authHeader)
    } catch {
      case e: Exception =>
        if (retryCount > 0) {
          logger.info(s"Retrying to create HTTP GET connection due to exception ${e.getStackTrace}")
          TimeUnit.SECONDS.sleep(config.livyActiveJobPollingIntervalInSeconds)
          createGetConnection(url, authHeader, retryCount - 1)
        } else {
          throw e
        }
    }
  }

  private def createPostConnection(url: String,
                                   authHeader: String,
                                   retryCount: Int = config.livyRetryCount): HttpURLConnection = {
    try {
      httpPostConnection(url, authHeader)
    } catch {
      case e: Exception =>
        if (retryCount > 0) {
          logger.info(s"Retrying to create HTTP POST connection due to exception" +
            s" ${e.getStackTrace}")
          TimeUnit.SECONDS.sleep(config.livyActiveJobPollingIntervalInSeconds)
          createGetConnection(url, authHeader, retryCount - 1)
        } else {
          throw e
        }
    }
  }

  private def httpGetConnection( url: String, authHeader: String): HttpURLConnection = {
    val httpURLConnection: HttpURLConnection =
      new URL(url).openConnection.asInstanceOf[HttpURLConnection]
    httpURLConnection.setRequestProperty(
      "Accept",
      "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
    )
    httpURLConnection.setDoOutput(true)
    httpURLConnection.setRequestMethod("GET")
    httpURLConnection.setRequestProperty("X-Requested-By", "admin")
    httpURLConnection.setRequestProperty("Authorization", authHeader)
    httpURLConnection
  }

  private def httpPostConnection(url: String,
                                 authHeader: String): HttpURLConnection = {
    val httpURLConnection: HttpURLConnection =
      new URL(url).openConnection.asInstanceOf[HttpURLConnection]
    httpURLConnection.setRequestProperty(
      "Accept",
      "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
    )
    httpURLConnection.setDoOutput(true)
    httpURLConnection.setRequestMethod("POST")
    httpURLConnection.setRequestProperty("Content-Type", "application/json")
    httpURLConnection.setRequestProperty("X-Requested-By", "admin")
    httpURLConnection.setRequestProperty("Authorization", authHeader)
    httpURLConnection
  }

}

object LivyUtil {
  def apply(): LivyUtil = new LivyUtil()
}
