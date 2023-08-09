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

  override def equals(other: Any): Boolean = {
    other match {
      case fact: Fact =>
        this.id == fact.id
      case _ => false
    }
  }

  override def hashCode(): Int = id.hashCode
}

case class FactBuilder(
  id: Long = null,
  personId: Integer = null,
  year: Integer = null,
  image: String = null,
  source: String = null,
  description: String = null,
  version: Long = null
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