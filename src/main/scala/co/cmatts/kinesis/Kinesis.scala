package co.cmatts.kinesis

import java.nio.charset.StandardCharsets.UTF_8
import java.util.{Timer, TimerTask}

import co.cmatts.client.Configuration.configureEndPoint
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.kinesis.model._
import software.amazon.awssdk.services.kinesis.{KinesisClient, KinesisClientBuilder}

import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

object Kinesis {
  private val MY_KEY: String = "myKey"

  private val receivedRecords: ListBuffer[Record] = ListBuffer[Record]()
  private val timer: Timer = new Timer("Kinesis Listener")

  private var client: Option[KinesisClient] = None
  private var streamName: Option[String] = None

  private def getKenisisClient: KinesisClient = {
    client match {
      case Some(client) => client
      case None =>
        val builder: KinesisClientBuilder = KinesisClient.builder
        configureEndPoint(builder)
        client = Option(builder.build)
        client.get
    }
  }

  def createStream(name: String, numberOfShards: Int): Unit = {
    if (streamName.isEmpty || streamName.get != name) {
      setStreamName(name)
      val createStreamRequest: CreateStreamRequest = CreateStreamRequest.builder.streamName(getStreamName).shardCount(numberOfShards).build
      getKenisisClient.createStream(createStreamRequest)
      waitForKinesisToBeActive()
    }
  }

  @throws[IllegalArgumentException]
  private def getStreamName: String = {
    streamName match {
      case Some(streamName) => streamName
      case None => throw new IllegalArgumentException("Stream Name must be set")
    }
  }

  private def setStreamName(name: String): Unit = {
    streamName = Option(name)
  }

  private def waitForKinesisToBeActive(): Unit = {
    val describeStreamRequest: DescribeStreamRequest = DescribeStreamRequest.builder.streamName(getStreamName).build
    getKenisisClient.waiter.waitUntilStreamExists(describeStreamRequest)
  }

  def getStreamStatus: StreamStatus = {
    val describeStreamRequest: DescribeStreamRequest = DescribeStreamRequest.builder.streamName(getStreamName).build
    getKenisisClient.describeStream(describeStreamRequest).streamDescription.streamStatus
  }

  def sendToKinesis(messages: List[String]): Unit = {
    val putRecords: List[PutRecordsRequestEntry] = messages.map(m => PutRecordsRequestEntry.builder
      .partitionKey(MY_KEY)
      .data(stringToSdkBytes(m))
      .build
    )
    val request: PutRecordsRequest = PutRecordsRequest.builder
      .streamName(getStreamName)
      .records(putRecords.asJava)
      .build
    getKenisisClient.putRecords(request)
  }

  private def stringToSdkBytes(message: String): SdkBytes = {
    SdkBytes.fromString(message, UTF_8)
  }

  def startKinesisListener(): Unit = {
    val task: TimerTask = new TimerTask() {
      override def run(): Unit = {
        readStream()
      }
    }
    val period: Long = 1000L
    timer.schedule(task, 0, period)
  }

  def stopKinesisListener(): Unit = {
    timer.cancel()
  }

  def getReceivedRecords: List[Record] = {
    val records: List[Record] = receivedRecords.toList
    if (records.nonEmpty) {
      receivedRecords.remove(0, records.size)
    }
    records
  }

  private def readStream(): Unit = {
    val listShardsRequest: ListShardsRequest = ListShardsRequest.builder
      .streamName(getStreamName)
      .build
    val shards: ListShardsResponse = getKenisisClient.listShards(listShardsRequest)
    for (shard <- shards.shards().asScala) {
      receivedRecords.addAll(readStream(shard))
    }
  }

  private def readStream(shard: Shard): List[Record] = {
    val getShardIteratorRequest: GetShardIteratorRequest = GetShardIteratorRequest.builder
      .streamName(getStreamName)
      .shardId(shard.shardId)
      .shardIteratorType(ShardIteratorType.TRIM_HORIZON)
      .build
    val getShardIteratorResponse: GetShardIteratorResponse = getKenisisClient.getShardIterator(getShardIteratorRequest)
    val shardIterator: String = getShardIteratorResponse.shardIterator
    val getRecordsRequest: GetRecordsRequest = GetRecordsRequest.builder
      .shardIterator(shardIterator)
      .limit(25)
      .build
    val getRecordsResponse: GetRecordsResponse = getKenisisClient.getRecords(getRecordsRequest)
    getRecordsResponse.records.asScala.toList
  }
}
