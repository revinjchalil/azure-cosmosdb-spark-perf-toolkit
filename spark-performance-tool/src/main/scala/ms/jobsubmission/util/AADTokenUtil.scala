package ms.jobsubmission.util

import java.util.Date
import java.util.concurrent.Executors
import java.util.concurrent.ExecutorService

import com.microsoft.aad.adal4j.AuthenticationContext
import com.microsoft.aad.adal4j.ClientCredential

/**
 * Get AAD access token using service principle.
 */
class AADTokenUtil(tenantId: String,
                   servicePrincipalId: String,
                   servicePrincipalSecret: String) {
  private val RESOURCE_URI = "https://dev.azuresynapse.net"
  private val executorService: ExecutorService =
    Executors.newSingleThreadExecutor()
  private val authorityUri = "https://login.microsoftonline.com/" + tenantId
  private val context =
    new AuthenticationContext(authorityUri, true, executorService)
  private var accessToken: String = null
  private var expiryDate: Date = new Date()

  def acquireToken(): String = {
    val date: Date = new Date()
//    if (accessToken == null || date.after(expiryDate)) {
//      val authResult = context.acquireToken(
//        RESOURCE_URI, servicePrincipalId, "rechalil@microsoft.com", "SparkingML2018#",
//        null
//      )

    if (accessToken == null || date.after(expiryDate)) {
      val authResult = context.acquireToken(
        RESOURCE_URI,
        new ClientCredential(servicePrincipalId, servicePrincipalSecret),
        null
      )
      accessToken = authResult.get.getAccessToken
      expiryDate = authResult.get.getExpiresOnDate
    }
    accessToken
  }

  def close(): Unit = {
    executorService.shutdown()
  }
}
