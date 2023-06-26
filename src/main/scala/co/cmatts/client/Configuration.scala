package co.cmatts.client

import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder
import software.amazon.awssdk.regions.Region

import java.net.URI

object Configuration {
  private val LOCAL_STACK_ENDPOINT = "LOCAL_STACK_ENDPOINT"
  private val AWS_REGION = "AWS_REGION"

  def configureEndPoint(builder: (AwsClientBuilder[BuilderT, ClientT]) forSome {type BuilderT <: AwsClientBuilder[BuilderT, ClientT]; type ClientT}): (AwsClientBuilder[BuilderT, ClientT]) forSome {type BuilderT <: AwsClientBuilder[BuilderT, ClientT]; type ClientT} = {
    val localS3Endpoint = System.getenv(LOCAL_STACK_ENDPOINT)
    val awsRegion = System.getenv(AWS_REGION)
    if (localS3Endpoint != null && awsRegion != null) {
      builder.region(Region.of(awsRegion))
      builder.endpointOverride(URI.create(localS3Endpoint))
    }
    builder
  }
}
