package co.cmatts.secretsmanager

import co.cmatts.localstack.LocalStackEnvironment.setupLocalStackEnvironment
import co.cmatts.secretsmanager.SecretsManager.{createSecret, readSecret, updateSecret}
import com.dimafeng.testcontainers.LocalStackV2Container
import com.dimafeng.testcontainers.scalatest.TestContainerForAll
import org.scalatest.BeforeAndAfter
import org.scalatest.funspec.AnyFunSpec
import org.testcontainers.containers.localstack.LocalStackContainer.Service

class SecretsManagerSpec extends AnyFunSpec with TestContainerForAll with BeforeAndAfter {
  private val SECRET_NAME = "MY_SECRET"
  private val SECRET_VALUE = "{ \"mySecret\": \"mySecretValue\" }"

  override val containerDef: LocalStackV2Container.Def = LocalStackV2Container.Def("2.1.0", Seq(Service.SECRETSMANAGER))

  before({
    withContainers(localStack =>
      setupLocalStackEnvironment(localStack.container)
    )
  })

  describe("SecretsManager test suite") {
    it("should create secrets") {
      assert(!createSecret(SECRET_NAME, SECRET_VALUE).isBlank)
    }

    it("should read secrets") {
      assert(readSecret(SECRET_NAME) === SECRET_VALUE)
    }

    it("should update secrets") {
      val secret = "{ \"mySecret\": \"mySecretValue\", \"myUpdateSecret\": \"myUpdatedSecretValue\"}";
      updateSecret(SECRET_NAME, secret);
      assert(readSecret(SECRET_NAME) === secret)
    }
  }
}
