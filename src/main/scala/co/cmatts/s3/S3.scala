package co.cmatts.s3

import java.io.InputStream
import java.nio.file.Path

import co.cmatts.client.Configuration.configureEndPoint
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model._

object S3 {
  private var SC_OK = 200
  private var client: S3Client = null

  def getS3Client: S3Client = {
    if (client != null) return client
    val builder = S3Client.builder
    configureEndPoint(builder)
    client = builder.build
    client
  }

  def resetS3Client(): Unit = {
    client = null
  }

  def bucketExists(bucket: String): Boolean = {
    val headBucketRequest = HeadBucketRequest.builder.bucket(bucket).build
    val headBucketResponse = getS3Client.headBucket(headBucketRequest)
    SC_OK == headBucketResponse.sdkHttpResponse.statusCode
  }

  def createBucket(bucket: String): Unit = {
    val createBuckerRequest = CreateBucketRequest.builder.bucket(bucket).build
    getS3Client.createBucket(createBuckerRequest)
    val headBucketRequest = HeadBucketRequest.builder.bucket(bucket).build
    getS3Client.waiter.waitUntilBucketExists(headBucketRequest)
  }

  @throws[IllegalArgumentException]
  def writeToBucket(bucket: String, key: String, path: Path): Unit = {
    val putObjectRequest = PutObjectRequest.builder.bucket(bucket).key(key).build
    getS3Client.putObject(putObjectRequest, path)
  }

  @throws[IllegalArgumentException]
  def writeToBucket(bucket: String, key: String, content: String): Unit = {
    val putObjectRequest = PutObjectRequest.builder.bucket(bucket).key(key).build
    val requestBody = RequestBody.fromString(content)
    getS3Client.putObject(putObjectRequest, requestBody)
  }

  def fileExists(bucket: String, key: String): Boolean = {
    val headObjectRequest = HeadObjectRequest.builder.bucket(bucket).key(key).build
    val headObjectResponse = getS3Client.headObject(headObjectRequest)
    SC_OK == headObjectResponse.sdkHttpResponse.statusCode
  }

  def readFromBucket(bucket: String, key: String): InputStream = {
    val getObjectRequest = GetObjectRequest.builder.bucket(bucket).key(key).build
    getS3Client.getObject(getObjectRequest)
  }

}
