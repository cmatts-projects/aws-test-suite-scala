package co.cmatts.dynamo.model

import software.amazon.awssdk.enhanced.dynamodb.extensions.annotations.DynamoDbVersionAttribute
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSecondaryPartitionKey

import scala.annotation.meta.beanGetter
import scala.beans.BeanProperty

@DynamoDbBean
case class Fact(
    @(DynamoDbPartitionKey @beanGetter)
    @BeanProperty var id: Long,
    @DynamoDbSecondaryPartitionKey(indexNames = Array("personIndex"))
    @BeanProperty var personId: Integer,
    @BeanProperty var year: Integer,
    @BeanProperty var image: String,
    @BeanProperty var source: String,
    @BeanProperty var description: String,
    @(DynamoDbVersionAttribute @beanGetter)
    @BeanProperty var version: Long
) extends DynamoDbMappedBean {

  override def tableName(): String = "facts"
}

case class FactBuilder(
  id: Long = -1,
  personId: Integer = -1,
  year: Integer = -1,
  image: String = "",
  source: String = "",
  description: String = "",
  version: Long = -1
) {
  def id(id: Long): FactBuilder = {
    copy(id = id)
  }

  def personId(personId: Integer): FactBuilder = {
    copy(personId = personId)
  }

  def year(year: Integer): FactBuilder = {
    copy(year = year)
  }

  def image(image: String): FactBuilder = {
    copy(image = image)
  }

  def source(source: String): FactBuilder = {
    copy(source = source)
  }

  def description(description: String): FactBuilder = {
    copy(description = description)
  }

  def version(version: Long): FactBuilder = {
    copy(version = version)
  }

  def build(): Fact = {
    Fact(
      id = id,
      personId = personId,
      year = year,
      image = image,
      source = source,
      description = description,
      version = version
    )
  }
}