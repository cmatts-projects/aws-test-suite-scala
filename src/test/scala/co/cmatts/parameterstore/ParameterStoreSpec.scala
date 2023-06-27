package co.cmatts.parameterstore

import co.cmatts.localstack.LocalStackEnvironment.setupLocalStackEnvironment
import com.dimafeng.testcontainers.LocalStackV2Container
import com.dimafeng.testcontainers.scalatest.TestContainerForAll
import org.scalatest.BeforeAndAfter
import org.scalatest.funspec.AnyFunSpec
import org.testcontainers.containers.localstack.LocalStackContainer.Service

class ParameterStoreSpec extends AnyFunSpec with TestContainerForAll with BeforeAndAfter {
  private val PARAMETER_NAME = "MY_PARAMETER"
  private val PARAMETER_VALUE = "A parameter value"
  private val PARAMETER_DESCRIPTION = "A description"

  override val containerDef: LocalStackV2Container.Def = LocalStackV2Container.Def("2.1.0", Seq(Service.SSM))

  before({
    withContainers(localStack =>
      setupLocalStackEnvironment(localStack.container)
    )
  })

  describe("ParameterStore test suite") {
    it("should access parameter") {
      ParameterStore.writeParameter(PARAMETER_NAME, PARAMETER_VALUE, PARAMETER_DESCRIPTION)
      assert(ParameterStore.readParameter(PARAMETER_NAME) === PARAMETER_VALUE)
    }
  }
}

