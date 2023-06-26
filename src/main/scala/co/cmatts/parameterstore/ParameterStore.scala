package co.cmatts.parameterstore

import co.cmatts.client.Configuration.configureEndPoint
import software.amazon.awssdk.services.ssm.SsmClient
import software.amazon.awssdk.services.ssm.model.{GetParameterRequest, ParameterType, PutParameterRequest}

object ParameterStore {
  private var client: SsmClient = null

  private def getParameterStoreClient: SsmClient = {
    if (client != null) return client
    val builder = SsmClient.builder
    configureEndPoint(builder)
    client = builder.build
    client
  }

  def writeParameter(parameterName: String, parameterValue: String, parameterDescription: String): Unit = {
    val parameterRequest = PutParameterRequest.builder.name(parameterName).value(parameterValue).`type`(ParameterType.STRING).description(parameterDescription).build
    getParameterStoreClient.putParameter(parameterRequest)
  }

  def readParameter(parameterName: String): String = {
    val parameterRequest = GetParameterRequest.builder.name(parameterName).build
    getParameterStoreClient.getParameter(parameterRequest).parameter.value
  }
}
