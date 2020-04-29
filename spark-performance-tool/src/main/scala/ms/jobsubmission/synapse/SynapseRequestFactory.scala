package ms.jobsubmission.synapse

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

import ms.jobsubmission.Job

class SynapseRequestBodyFactory(jsonMapper: ObjectMapper with ScalaObjectMapper) {

  def getSynapseRequest(job: Job): String = {
    val numExecutors = job.conf.getOrElse("spark.executor.instances", "0").toInt
    val executorCores = job.conf.getOrElse("spark.executor.cores", "0").toInt
    val executorMemory = job.conf.getOrElse("spark.executor.memory", "0")
    val driverCores = job.conf.getOrElse("spark.driver.cores", "0").toInt
    val driverMemory = job.conf.getOrElse("spark.driver.memory", "0")

    val synapseRequestBody = SynapseRequestBody(job.name,
      job.className,
      job.jarFilePath,
      job.applicationArgs,
      job.conf,
      numExecutors,
      executorCores,
      executorMemory,
      driverCores,
      driverMemory
    )
    jsonMapper.writeValueAsString(synapseRequestBody)
  }

}

object SynapseRequestBodyFactory {
  val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)
  mapper.setSerializationInclusion(Include.NON_NULL)
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  def apply(): SynapseRequestBodyFactory = new SynapseRequestBodyFactory(mapper)
}

/**
*
 * @param name Synapse Job Name
 * @param className Main class
 * @param file Jar file
 * @param args Args to the main class
 * @param conf spark conf
 * @param numExecutors Number of executors(Mandatory param)
 * @param executorCores Number of cores per executor(Mandatory param)
 * @param executorMemory Memory per executor((Mandatory param)
 * @param driverCores Number of cores for driver(Mandatory param)
 * @param driverMemory Memory for driver(Mandatory param)
 */
case class SynapseRequestBody(name: String,
                              className: String,
                              file: String,
                              args: Array[String],
                              conf: Map[String, String],
                              numExecutors: Int,
                              executorCores: Int,
                              executorMemory: String,
                              driverCores: Int,
                              driverMemory: String)
