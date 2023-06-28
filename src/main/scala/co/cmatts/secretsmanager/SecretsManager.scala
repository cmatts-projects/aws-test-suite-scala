package co.cmatts.secretsmanager

import co.cmatts.client.Configuration.configureEndPoint
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient
import software.amazon.awssdk.services.secretsmanager.model.{CreateSecretRequest, GetSecretValueRequest, PutSecretValueRequest}

object SecretsManager {
  private var client: Option[SecretsManagerClient] = None

  private def getSecretsManagerClient: SecretsManagerClient = {
    client match {
      case Some(client) => client
      case None =>
        val builder = SecretsManagerClient.builder
        configureEndPoint(builder)
        client = Option(builder.build)
        client.get
    }
  }

  def createSecret(secretName: String, secretValue: String): String = {
    val secretRequest = CreateSecretRequest.builder.name(secretName).secretString(secretValue).build
    val result = getSecretsManagerClient.createSecret(secretRequest)
    result.arn
  }

  def updateSecret(secretName: String, secretValue: String): Unit = {
    val secretValueRequest = PutSecretValueRequest.builder.secretId(secretName).secretString(secretValue).build
    getSecretsManagerClient.putSecretValue(secretValueRequest)
  }

  def readSecret(secretName: String): String = {
    val secretValueRequest = GetSecretValueRequest.builder.secretId(secretName).build
    getSecretsManagerClient.getSecretValue(secretValueRequest).secretString
  }

}
