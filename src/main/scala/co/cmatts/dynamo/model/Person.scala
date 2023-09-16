package co.cmatts.dynamo.model

import software.amazon.awssdk.enhanced.dynamodb.extensions.annotations.DynamoDbVersionAttribute
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.{DynamoDbBean, DynamoDbPartitionKey, DynamoDbSecondaryPartitionKey}

import scala.annotation.meta.beanGetter
import scala.beans.BeanProperty

@DynamoDbBean
class Person(
    @(DynamoDbPartitionKey @beanGetter)
    @BeanProperty var id: Long,
    @BeanProperty var name: String,
    @BeanProperty var yearOfBirth: Integer,
    @BeanProperty var yearOfDeath: Integer,
    @DynamoDbSecondaryPartitionKey(indexNames = Array("fatherIndex"))
    @BeanProperty var fatherId: Integer,
    @DynamoDbSecondaryPartitionKey(indexNames = Array("motherIndex"))
    @BeanProperty var motherId: Integer,
    @DynamoDbVersionAttribute
    @BeanProperty var version: Long
) extends DynamoDbMappedBean {

  override def tableName(): String = "people"

  override def equals(other: Any): Boolean = {
    other match {
      case person: Person =>
        this.id == person.id
      case _ => false
    }
  }

  override def hashCode(): Int = id.hashCode
}

case class PersonBuilder(
  id: Long,
  name: String,
  yearOfBirth: Integer,
  yearOfDeath: Integer,
  fatherId: Integer,
  motherId: Integer,
  version: Long
) {

  def this() = {
    this(null, null, null, null, null, null, null)
  }

  def id(id: Long): PersonBuilder = {
    copy(id = id)
  }

  def name(name: String): PersonBuilder = {
    copy(name = name)
  }

  def yearOfBirth(yearOfBirth: Integer): PersonBuilder = {
    copy(yearOfBirth = yearOfBirth)
  }

  def yearOfDeath(yearOfDeath: Integer): PersonBuilder = {
    copy(yearOfDeath = yearOfDeath)
  }

  def fatherId(fatherId: Integer): PersonBuilder = {
    copy(fatherId = fatherId)
  }

  def motherId(motherId: Integer): PersonBuilder = {
    copy(motherId = motherId)
  }

  def version(version: Long): PersonBuilder = {
    copy(version = version)
  }

  def build(): Person = {
    new Person(
      id = id,
      name = name,
      yearOfBirth = yearOfBirth,
      yearOfDeath = yearOfDeath,
      fatherId = fatherId,
      motherId = motherId,
      version = version
    )
  }
}