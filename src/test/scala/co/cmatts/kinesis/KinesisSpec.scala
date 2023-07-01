package co.cmatts.kinesis

import java.nio.charset.StandardCharsets.UTF_8

import co.cmatts.kinesis.Kinesis._
import co.cmatts.localstack.LocalStackEnvironment.setupLocalStackEnvironment
import com.dimafeng.testcontainers.LocalStackV2Container
import com.dimafeng.testcontainers.scalatest.TestContainerForAll
import org.scalatest.BeforeAndAfter
import org.scalatest.funspec.AnyFunSpec
import org.testcontainers.containers.localstack.LocalStackContainer.Service
import software.amazon.awssdk.core.SdkSystemSetting
import software.amazon.awssdk.services.kinesis.model.{Record, StreamStatus}
import uk.org.webcompere.systemstubs.properties.SystemProperties

import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class KinesisSpec extends AnyFunSpec with TestContainerForAll with BeforeAndAfter {
  private val MY_STREAM = "myStream"
  private val A_MESSAGE = "A message"
  private val ANOTHER_MESSAGE = "Another message"

  override val containerDef: LocalStackV2Container.Def = LocalStackV2Container.Def("2.1.0", Seq(Service.SSM))

  before({
    withContainers(localStack => {
      setupLocalStackEnvironment(localStack.container)
      new SystemProperties()
        .set(SdkSystemSetting.CBOR_ENABLED.property, "false")
    })

    createStream(MY_STREAM, 1)
  })

  def retrieveRecordsFromKinesis(numberOfRecords: Int): List[Record] = {
    val messages: ListBuffer[Record] = ListBuffer[Record]()
    val readMessages: Future[Unit] = Future(
      while (messages.size < numberOfRecords) {
        messages.addAll(getReceivedRecords)
        Thread.sleep(100)
      }
    )
    Await.result(readMessages, 5.seconds)
    messages.toList
  }

  describe("Kinesis test suite") {
    it("should be active kinesis stream") {
      assert(getStreamStatus === StreamStatus.ACTIVE)
    }

    it("should send and retrieve message with stream") {
      startKinesisListener()
      sendToKinesis(List(A_MESSAGE, ANOTHER_MESSAGE))
      val records: List[Record] = retrieveRecordsFromKinesis(2)
      stopKinesisListener()
      val receivedMessages: List[String] = records.map(r => UTF_8.decode(r.data.asByteBuffer).toString)
      assert(receivedMessages.contains(A_MESSAGE))
      assert(receivedMessages.contains(ANOTHER_MESSAGE))
      val moreRecords: List[Record] = getReceivedRecords
      assert(moreRecords.size === 0)
    }
  }
}
