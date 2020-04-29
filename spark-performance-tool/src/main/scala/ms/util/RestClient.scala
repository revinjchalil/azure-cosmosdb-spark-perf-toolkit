package ms.util

import java.io.IOException

import org.apache.http.{HttpResponse, HttpResponseInterceptor}
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.{ContentType, StringEntity}
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.protocol.HttpContext
import org.apache.spark.internal.Logging

/**
 * A http client for making rest calls
 */
class RestClient extends Logging{

  def postRequest(url: String, jsonContent: String = ""): Option[String] = {
    logInfo(s"jsonContent is $jsonContent")
    val requestEntity = new StringEntity(jsonContent, ContentType.APPLICATION_JSON)
    val postMethod = new HttpPost(url)
    postMethod.setEntity(requestEntity)
    var content: Option[String] = None
    var httpRestClient: DefaultHttpClient = null
    try {
      httpRestClient = new DefaultHttpClient()
      httpRestClient.addResponseInterceptor(new HttpResponseInterceptor {
        override def process(httpResponse: HttpResponse, httpContext: HttpContext): Unit = {
          if (httpResponse.getStatusLine.getStatusCode.toString.startsWith("5") ||
            httpResponse.getStatusLine.getStatusCode == 429 ) {
            logWarning(s"Response returned with status code: ${httpResponse.getStatusLine
              .getStatusCode}, will throw an exception to retry request")
            throw new IOException("Retrying failed HTTP request")
          }
        }
      })
      val httpResponse = httpRestClient.execute(postMethod)
      val entity = httpResponse.getEntity
      if (entity != null) {
        val inputStream = entity.getContent
        content = Some(scala.io.Source.fromInputStream(inputStream).mkString)
        inputStream.close
      }
    } catch {
      case e: Exception =>
        logWarning(s"Exception thrown making a http request, ${e.getMessage}")
        throw new Exception(s"exception while making a post request")
    } finally {
      httpRestClient.getConnectionManager.shutdown
    }
    content
  }
}
