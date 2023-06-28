package co.cmatts.parameterstore

import co.cmatts.client.Configuration.configureEndPoint
import software.amazon.awssdk.services.ssm.SsmClient
import software.amazon.awssdk.services.ssm.model.{GetParameterRequest, ParameterType, PutParameterRequest}

object ParameterStore {
  private var client: Option[SsmClient] = None

  private def getParameterStoreClient: SsmClient = {
    client match {
      case Some(client) => client
      case None =>
        val builder = SsmClient.builder
        configureEndPoint(builder)
        client = Option(builder.build)
        client.get
    }
  }

  def writeParameter(parameterName: String, parameterValue: String, parameterDescription: String): Unit = {
    val parameterRequest = PutParameterRequest.builder
      .name(parameterName)
      .value(parameterValue)
      .`type`(ParameterType.STRING)
      .description(parameterDescription)
      .build
    getParameterStoreClient.putParameter(parameterRequest)
  }

  def readParameter(parameterName: String): String = {
    val parameterRequest = GetParameterRequest.builder
      .name(parameterName)
      .build
    getParameterStoreClient.getParameter(parameterRequest).parameter.value
  }
}
