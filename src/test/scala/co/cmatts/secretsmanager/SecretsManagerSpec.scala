package co.cmatts.secretsmanager

import co.cmatts.localstack.LocalStackEnvironment.withLocalStackEnvironment
import co.cmatts.secretsmanager.SecretsManager.{createSecret, readSecret, updateSecret}
import com.dimafeng.testcontainers.LocalStackV2Container
import com.dimafeng.testcontainers.scalatest.TestContainerForAll
import org.scalatest.funspec.AnyFunSpec
import org.testcontainers.containers.localstack.LocalStackContainer.Service

class SecretsManagerSpec extends AnyFunSpec with TestContainerForAll {
  private val SECRET_NAME = "MY_SECRET"
  private val SECRET_VALUE = "{ \"mySecret\": \"mySecretValue\" }"

  override val containerDef: LocalStackV2Container.Def = LocalStackV2Container.Def("2.1.0", Seq(Service.SECRETSMANAGER))

  describe("SecretsManager test suite") {

    it("should create secrets") {
      withContainers(localstack => {
        withLocalStackEnvironment(localstack.container, () => {
          assert(!createSecret(SECRET_NAME, SECRET_VALUE).isBlank)
        })
      })
    }

    it("should read secrets") {
      withContainers(localstack => {
        withLocalStackEnvironment(localstack.container, () => {
          assert(readSecret(SECRET_NAME) === SECRET_VALUE)
        })
      })
    }

    it("should update secrets") {
      withContainers(localstack => {
        withLocalStackEnvironment(localstack.container, () => {
          val secret = "{ \"mySecret\": \"mySecretValue\", \"myUpdateSecret\": \"myUpdatedSecretValue\"}";
          updateSecret(SECRET_NAME, secret);
          assert(readSecret(SECRET_NAME) === secret)
        })
      })
    }

  }
}
