package co.cmatts.cloudwatch

import java.time.ZoneOffset.UTC
import java.time.{Instant, LocalDateTime}
import java.util.concurrent.TimeUnit

import co.cmatts.client.Configuration.configureEndPoint
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient
import software.amazon.awssdk.services.cloudwatch.model._

import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

object Cloudwatch {
  private var client: Option[CloudWatchClient] = None

  private def getCloudWatchClient: CloudWatchClient = {
    client match {
      case Some(client) => client
      case None =>
        val builder = CloudWatchClient.builder
        configureEndPoint(builder)
        client = Option(builder.build)
        client.get
    }
  }

  def createMetric(dimensionName: String, dimensionValue: String, metricName: String,
                   value: Int,timestamp: Instant): MetricDatum = {
    val dimension: Dimension = Dimension.builder
      .name(dimensionName)
      .value(dimensionValue)
      .build
    MetricDatum.builder
      .metricName(metricName)
      .value(value.toDouble)
      .unit(StandardUnit.COUNT)
      .dimensions(dimension)
      .timestamp(timestamp)
      .build
  }

  def logMetrics(metrics: List[MetricDatum], namespace: String): List[PutMetricDataResponse] = {
    val response: ListBuffer[PutMetricDataResponse] = ListBuffer[PutMetricDataResponse]()

    for (metricsBatch: List[MetricDatum] <- metrics.grouped(25).toList) {
      val request: PutMetricDataRequest = PutMetricDataRequest.builder
        .namespace(namespace)
        .metricData(metricsBatch.asJava)
        .build
      response += getCloudWatchClient.putMetricData(request)
    }

    response.toList
  }

  def getAverageForDays(days: Int, dimensionName: String, dimensionValue: String,
                        namespace: String, metricName: String): Double = {
    val now: LocalDateTime = LocalDateTime.now
    val startTime: Instant = now.minusDays(days).toInstant(UTC)
    val endTime: Instant = now.toInstant(UTC)

    val dimension = Dimension.builder
      .name(dimensionName)
      .value(dimensionValue)
      .build

    val request: GetMetricStatisticsRequest = GetMetricStatisticsRequest.builder
      .startTime(startTime)
      .endTime(endTime)
      .period(TimeUnit.DAYS.toSeconds(days).toInt)
      .statistics(Statistic.AVERAGE)
      .namespace(namespace)
      .metricName(metricName)
      .dimensions(dimension)
      .build

    val response: GetMetricStatisticsResponse =
      getCloudWatchClient.getMetricStatistics(request)

    val datapoint = response.datapoints.asScala.headOption
    datapoint match {
      case Some(d) => d.average
      case None    => 0d
    }
  }

}
