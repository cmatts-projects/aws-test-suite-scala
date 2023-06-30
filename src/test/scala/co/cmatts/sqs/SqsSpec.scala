package co.cmatts.sqs

import co.cmatts.localstack.LocalStackEnvironment.setupLocalStackEnvironment
import co.cmatts.s3.S3.{createBucket, resetS3Client}
import co.cmatts.sqs.Sqs._
import com.dimafeng.testcontainers.LocalStackV2Container
import com.dimafeng.testcontainers.scalatest.TestContainerForAll
import org.scalatest.BeforeAndAfter
import org.scalatest.funspec.AnyFunSpec
import org.testcontainers.containers.localstack.LocalStackContainer.Service

import scala.collection.mutable.ListBuffer

class SqsSpec extends AnyFunSpec with TestContainerForAll with BeforeAndAfter {
  private val TEST_QUEUE_BUCKET = "my-queue-bucket"
  private val TEST_QUEUE = "myQueue"
  private val TEST_MESSAGE = "A test message"
  private val EXTENDED_MESSAGE_PAYLOAD = "\\[\"software.amazon.payloadoffloading.PayloadS3Pointer\",\\{\"s3BucketName\":\"my-queue-bucket\",\"s3Key\":\".*\"\\}\\]"


  override val containerDef: LocalStackV2Container.Def = LocalStackV2Container.Def("2.1.0", Seq(Service.SQS, Service.S3))

  before({
    withContainers(localStack =>
      setupLocalStackEnvironment(localStack.container)
    )
    resetS3Client()
    createBucket(TEST_QUEUE_BUCKET)
    setExtendedClientBucket(TEST_QUEUE_BUCKET)
    createQueue(TEST_QUEUE)
    purgeQueue(TEST_QUEUE)
  })

  def retrieveMessagesFromSqs(numberOfRecords: Int): List[String] = {
    val messages: ListBuffer[String] = ListBuffer[String]()
    var tries = 0
    while (messages.size < numberOfRecords && tries < 50) {
      messages.addAll(readFromQueue(TEST_QUEUE))
      Thread.sleep(100)
      tries += 1
    }
    messages.toList
  }

  describe("Sqs test suite") {
    it("should send to queue") {
      sendToQueue(TEST_QUEUE, TEST_MESSAGE)

      val receivedMessages = readFromQueue(TEST_QUEUE)
      assert(receivedMessages.size === 1)
      assert(receivedMessages.head === TEST_MESSAGE)
    }

    it("should send simple message to extended queue") {
      sendToExtendedQueue(TEST_QUEUE, TEST_MESSAGE)

      val receivedMessages: List[String] = readFromQueue(TEST_QUEUE)
      assert(receivedMessages.size === 1)
      assert(receivedMessages.head === TEST_MESSAGE)
    }

    it("should send large message to extended queue") {
      val largeMessage: String = "X" * (257 * 1024)
      sendToExtendedQueue(TEST_QUEUE, largeMessage)

      val receivedMessages: List[String] = readFromQueue(TEST_QUEUE)
      assert(receivedMessages.size === 1)
      assert(EXTENDED_MESSAGE_PAYLOAD.r.matches(receivedMessages.head))
    }

    it("should receive large message from extended queuet") {
      val largeMessage: String = "X" * (257 * 1024)
      sendToExtendedQueue(TEST_QUEUE, largeMessage)

      val receivedMessages: List[String] = readFromExtendedQueue(TEST_QUEUE)
      assert(receivedMessages.size === 1)
      assert(receivedMessages.head === largeMessage)
    }

    it("should split message batch when batch exceeds max bytes") {
      val largeMessage: String = "X" * (127 * 1024)
      val messageBatch: List[String] = List(largeMessage, largeMessage, largeMessage)
      sendToExtendedQueue(TEST_QUEUE, messageBatch)

      val receivedMessages: List[String] = retrieveMessagesFromSqs(3)
      for (message: String <- receivedMessages) assert(message === largeMessage)
    }

    it("should split message batch when batch exceeds max messages") {
      val messageBatch: List[String] = List.fill(26)(TEST_MESSAGE)
      sendToExtendedQueue(TEST_QUEUE, messageBatch)

      val receivedMessages: List[String] = retrieveMessagesFromSqs(26)
      for (message: String <- receivedMessages) assert(message === TEST_MESSAGE)
    }
  }
}
