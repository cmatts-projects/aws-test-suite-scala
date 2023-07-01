package co.cmatts.localstack

import org.testcontainers.containers.localstack.LocalStackContainer
import uk.org.webcompere.systemstubs.environment.EnvironmentVariables

object LocalStackEnvironment {

  def setupLocalStackEnvironment(localStackContainer: LocalStackContainer) {
    new EnvironmentVariables()
      .set("AWS_ACCESS_KEY_ID", localStackContainer.getAccessKey)
      .set("AWS_SECRET_ACCESS_KEY", localStackContainer.getSecretKey)
      .set("LOCAL_STACK_ENDPOINT", localStackContainer.getEndpointOverride(null).toString)
      .set("AWS_REGION", localStackContainer.getRegion)
      .setup()
  }

}
