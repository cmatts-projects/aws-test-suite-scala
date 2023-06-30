package co.cmatts.sqs

import java.nio.charset.StandardCharsets
import java.util.UUID.randomUUID

import co.cmatts.client.Configuration.configureEndPoint
import co.cmatts.s3.S3.getS3Client
import com.amazon.sqs.javamessaging.{AmazonSQSExtendedClient, ExtendedClientConfiguration}
import software.amazon.awssdk.services.sqs.SqsClient
import software.amazon.awssdk.services.sqs.model._
import software.amazon.payloadoffloading.{S3BackedPayloadStore, S3Dao}

import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

object Sqs {
  private val MAX_MESSAGE_BYTES = 256000
  private val MAX_BATCH_SIZE = 10

  private var client: Option[SqsClient] = None
  private var extendedClient: Option[SqsClient] = None
  private var extendedClientBucket: Option[String] = None
  private var payloadStore: Option[S3BackedPayloadStore] = None

  private def getSqsClient: SqsClient = {
    client match {
      case Some(client) => client
      case None =>
        val builder = SqsClient.builder
        configureEndPoint(builder)
        client = Option(builder.build)
        client.get
    }
  }

  @throws[IllegalArgumentException]
  private def getSqsExtendedClient: SqsClient = {
    if (extendedClientBucket == None) throw new IllegalArgumentException("Sqs extended client bucket not configured")

    extendedClient match {
      case Some(client) => client
      case None =>
        val extendedClientConfig = new ExtendedClientConfiguration().withPayloadSupportEnabled(getS3Client, extendedClientBucket.get)

        extendedClient = Option(new AmazonSQSExtendedClient(getSqsClient, extendedClientConfig))

        extendedClient.get
    }
  }

  private def getPayloadStore: S3BackedPayloadStore = {
    payloadStore match {
      case Some(payloadStore) => payloadStore
      case None =>
        val s3Dao: S3Dao = new S3Dao(getS3Client)
        payloadStore = Option(new S3BackedPayloadStore(s3Dao, extendedClientBucket.get))
        payloadStore.get
    }
  }

  def setExtendedClientBucket(extendedClientBucket: String): Unit = {
    this.extendedClientBucket = Option(extendedClientBucket)
    extendedClient = None
    payloadStore = None
  }

  def createQueue(queueName: String): Unit = {
    val createQueueRequest: CreateQueueRequest = CreateQueueRequest.builder
      .queueName(queueName)
      .build
    getSqsClient.createQueue(createQueueRequest)
  }

  def purgeQueue(queueName: String): Unit = {
    val purgeRequest: PurgeQueueRequest = PurgeQueueRequest.builder
      .queueUrl(getQueueUrl(queueName))
      .build
    getSqsClient.purgeQueue(purgeRequest)
  }

  def sendToQueue(queueName: String, message: String): Unit = {
    val sendMessageRequest: SendMessageRequest = SendMessageRequest.builder
      .queueUrl(getQueueUrl(queueName))
      .messageBody(message)
      .build
    getSqsClient.sendMessage(sendMessageRequest)
  }

  def readFromQueue(queueName: String): List[String] = {
    val receiveMessageRequest: ReceiveMessageRequest = ReceiveMessageRequest.builder
      .queueUrl(getQueueUrl(queueName))
      .build
    getSqsClient.receiveMessage(receiveMessageRequest)
      .messages
      .asScala
      .map(_.body)
      .toList
  }

  def sendToExtendedQueue(queueName: String, message: String): Unit = {
    val sendMessageRequest: SendMessageRequest = SendMessageRequest.builder.queueUrl(getQueueUrl(queueName)).messageBody(message).build
    getSqsExtendedClient.sendMessage(sendMessageRequest)
  }

  def sendToExtendedQueue(queueName: String, messages: List[String]): Unit = {
    val queueUrl: String = getQueueUrl(queueName)
    val batches: List[List[SendMessageBatchRequestEntry]] = splitBatchRequests(messages)

    for(batchEntries: List[SendMessageBatchRequestEntry] <- batches) {
      val sendMessageBatchRequest: SendMessageBatchRequest = SendMessageBatchRequest.builder
        .queueUrl(queueUrl)
        .entries(batchEntries.asJava)
        .build
      getSqsExtendedClient.sendMessageBatch(sendMessageBatchRequest)
    }
  }

  private def splitBatchRequests(messages: List[String]): List[List[SendMessageBatchRequestEntry]] = {
    var batches: ListBuffer[List[SendMessageBatchRequestEntry]] = ListBuffer[List[SendMessageBatchRequestEntry]]()
    var batchEntries: ListBuffer[SendMessageBatchRequestEntry] = ListBuffer[SendMessageBatchRequestEntry]()
    var currentBatchByteSize: Int = 0

    for (m <- messages) {
      val batchRequest: SendMessageBatchRequestEntry = SendMessageBatchRequestEntry.builder
        .id(randomUUID.toString)
        .messageBody(m)
        .build
      val batchRequestByteSize: Int = batchRequest.toString.getBytes(StandardCharsets.UTF_8).length

      if (MAX_MESSAGE_BYTES < (currentBatchByteSize + batchRequestByteSize) || MAX_BATCH_SIZE <= batchEntries.size) {
        batches += batchEntries.toList
        batchEntries = ListBuffer[SendMessageBatchRequestEntry]()
        currentBatchByteSize = 0
      }

      currentBatchByteSize += batchRequestByteSize
      batchEntries += batchRequest
    }
    batches += batchEntries.toList
    batches.toList
  }

  def readFromExtendedQueue(queueName: String): List[String] = {
    val receiveMessageRequest: ReceiveMessageRequest = ReceiveMessageRequest.builder
      .queueUrl(getQueueUrl(queueName))
      .build
    getSqsExtendedClient.receiveMessage(receiveMessageRequest)
      .messages
      .asScala
      .map(_.body)
      .toList
  }

  def toOriginalMessage(message: String): String = {
    getPayloadStore.getOriginalPayload(message)
  }

  def deleteOriginalMessage(message: String): Unit = {
    getPayloadStore.deleteOriginalPayload(message)
  }

  def storeOriginalMessage(message: String): String = {
    getPayloadStore.storeOriginalPayload(message)
  }

  private def getQueueUrl(queueName: String): String = {
    val getQueueAttributesRequest: GetQueueUrlRequest = GetQueueUrlRequest.builder
      .queueName(queueName)
      .build
    getSqsClient.getQueueUrl(getQueueAttributesRequest)
      .queueUrl
  }

}
