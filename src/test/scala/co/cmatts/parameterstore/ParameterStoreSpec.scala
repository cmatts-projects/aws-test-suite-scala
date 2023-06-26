package co.cmatts.parameterstore

import co.cmatts.localstack.LocalStackEnvironment.withLocalStackEnvironment
import com.dimafeng.testcontainers.LocalStackV2Container
import com.dimafeng.testcontainers.scalatest.TestContainerForAll
import org.scalatest.funspec.AnyFunSpec
import org.testcontainers.containers.localstack.LocalStackContainer.Service

class ParameterStoreSpec extends AnyFunSpec with TestContainerForAll {
  private val PARAMETER_NAME = "MY_PARAMETER"
  private val PARAMETER_VALUE = "A parameter value"
  private val PARAMETER_DESCRIPTION = "A description"

  override val containerDef: LocalStackV2Container.Def = LocalStackV2Container.Def("2.1.0", Seq(Service.SSM))

  describe("ParameterStore test suite") {

    it("should access parameter") {
      withContainers(localstack => {
        withLocalStackEnvironment(localstack.container, () => {

          ParameterStore.writeParameter(PARAMETER_NAME, PARAMETER_VALUE, PARAMETER_DESCRIPTION)
          assert(ParameterStore.readParameter(PARAMETER_NAME) === PARAMETER_VALUE)

        })
      })
    }
  }
}

