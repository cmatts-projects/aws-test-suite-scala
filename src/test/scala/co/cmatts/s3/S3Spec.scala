package co.cmatts.s3

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Paths

import co.cmatts.localstack.LocalStackEnvironment.withLocalStackEnvironment
import co.cmatts.s3.S3._
import com.dimafeng.testcontainers.LocalStackV2Container
import com.dimafeng.testcontainers.scalatest.TestContainerForAll
import org.scalatest.BeforeAndAfter
import org.scalatest.funspec.AnyFunSpec
import org.testcontainers.containers.localstack.LocalStackContainer.Service

import scala.util.Using

class S3Spec extends AnyFunSpec with TestContainerForAll with BeforeAndAfter {

  private val TEST_BUCKET = "mybucket"
  private val TEST_CONTENT = "{ \"content\": \"some content\" }"

  override val containerDef: LocalStackV2Container.Def =
    LocalStackV2Container.Def("2.1.0", Seq(Service.S3))

   before(
    withContainers(localstack => {
      withLocalStackEnvironment(localstack.container, () => {
        resetS3Client()
        createBucket(TEST_BUCKET)
      })
    })
   )

  describe("ParameterStore test suite") {

    it("should check bucket exist") {
      withContainers(localstack => {
        withLocalStackEnvironment(localstack.container, () => {
          assert(bucketExists(TEST_BUCKET))
        })
      })
    }

    it("should write file to bucket") {
      withContainers(localstack => {
        withLocalStackEnvironment(localstack.container, () => {
          val bucket = "mybucket"
          val key = "/test/resources/MyFile.txt"
          val localFile = Paths.get(this.getClass.getClassLoader.getResource("MyFile.txt").toURI)
          writeToBucket(bucket, key, localFile)

          assert(fileExists(bucket, key))
        })
      })
    }

    it("should write string to bucket") {
      withContainers(localstack => {
        withLocalStackEnvironment(localstack.container, () => {
          val bucket = "mybucket"
          val key = "/test/resources/MyContent.txt"
          writeToBucket(bucket, key, TEST_CONTENT)

          assert(fileExists(bucket, key))
        })
      })
    }

    it("should read from bucket") {
      withContainers(localstack => {
        withLocalStackEnvironment(localstack.container, () => {
          val bucket = "mybucket"
          val key = "/test/resources/readFile.txt"
          writeToBucket(bucket, key, TEST_CONTENT)

          Using(readFromBucket(bucket, key)) {
            s3InputStream => {
              val actualFileContent = new String(s3InputStream.readAllBytes, UTF_8)
              assert(actualFileContent === TEST_CONTENT)
            }
          }
        })
      })
    }
  }
}
