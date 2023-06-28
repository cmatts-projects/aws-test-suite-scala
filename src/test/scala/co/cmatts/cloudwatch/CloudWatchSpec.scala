package co.cmatts.cloudwatch

import java.time.ZoneOffset.UTC
import java.time.{Instant, LocalDateTime}

import co.cmatts.cloudwatch.Cloudwatch.{createMetric, getAverageForDays, logMetrics}
import co.cmatts.localstack.LocalStackEnvironment.setupLocalStackEnvironment
import com.dimafeng.testcontainers.LocalStackV2Container
import com.dimafeng.testcontainers.scalatest.TestContainerForAll
import org.scalatest.BeforeAndAfter
import org.scalatest.funspec.AnyFunSpec
import org.testcontainers.containers.localstack.LocalStackContainer.Service
import software.amazon.awssdk.services.cloudwatch.model.{MetricDatum, PutMetricDataResponse}
import specs2.arguments.sequential

import scala.collection.mutable.ListBuffer

class CloudWatchSpec extends AnyFunSpec with TestContainerForAll with BeforeAndAfter {
  sequential

  private val NUMBER_METRICS: Int = 100
  private val MY_DIMENSION: String = "myDimension"
  private val MY_COUNT: String = "myCount"
  private val MY_NAMESPACE: String = "myNamespace"
  private val MY_METRIC_NAME: String = "myMetricName"

  override val containerDef: LocalStackV2Container.Def =
    LocalStackV2Container.Def("2.1.0", Seq(Service.CLOUDWATCH))

  before({
    withContainers(localStack =>
      setupLocalStackEnvironment(localStack.container)
    )
  })

  describe("CloudWatch test suite") {
    it("should log metrics") {
      val metrics: ListBuffer[MetricDatum] = ListBuffer[MetricDatum]()
      for (i <- 0 until NUMBER_METRICS) {
        val timestamp: Instant = LocalDateTime.now.minusDays(i).toInstant(UTC)
        metrics += createMetric(MY_DIMENSION, MY_COUNT, MY_METRIC_NAME, i, timestamp)
      }
      val response: List[PutMetricDataResponse] = logMetrics(metrics.toList, MY_NAMESPACE)
      val expectedBatches: Int = (NUMBER_METRICS + 24) / 25

      assert(response.size === expectedBatches)

      for (r: PutMetricDataResponse <- response)
        assert(r.sdkHttpResponse.statusCode === 200)
    }

    it("should get metrics") {
      assert(getAverageForDays(30, MY_DIMENSION, MY_COUNT, MY_NAMESPACE, MY_METRIC_NAME) === 14.5d)
    }
  }
}
