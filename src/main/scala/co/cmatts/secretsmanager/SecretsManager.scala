package co.cmatts.secretsmanager

import co.cmatts.client.Configuration.configureEndPoint
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient
import software.amazon.awssdk.services.secretsmanager.model.{CreateSecretRequest, GetSecretValueRequest, PutSecretValueRequest}

object SecretsManager {
  private var client: SecretsManagerClient = null

  private def getSecretsManagerClient: SecretsManagerClient = {
    if (client != null) return client
    val builder = SecretsManagerClient.builder
    configureEndPoint(builder)
    client = builder.build
    client
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
