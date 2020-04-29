package ms.jobsubmission.hdi

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

import ms.jobsubmission.Job

/**
 * Factory class of getting a livy Request
 * @param jsonMapper
 */
class HdiRequestBodyFactory(jsonMapper: ObjectMapper with ScalaObjectMapper) {

  def getHdiRequest(job: Job): String = {
    val hdiRequestBody = HdiRequestBody(job.className,
      job.jarFilePath,
      job.applicationArgs,
      job.conf)
    jsonMapper.writeValueAsString(hdiRequestBody)
  }

}

object HdiRequestBodyFactory{
  val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)
  mapper.setSerializationInclusion(Include.NON_NULL)
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  def apply(): HdiRequestBodyFactory = new HdiRequestBodyFactory(mapper)
}

/**
 * A livy batch request body
 * @param className name for application class to be run
 * @param file path to the application jar
 * @param args arguments to be supplied to the application
 * @param conf spark confs
 */
case class HdiRequestBody(className: String,
                          file: String,
                          args: Array[String],
                          conf: Map[String, String])
