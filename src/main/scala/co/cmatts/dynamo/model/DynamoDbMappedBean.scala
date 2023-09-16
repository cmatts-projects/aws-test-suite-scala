package co.cmatts.dynamo.model

abstract class DynamoDbMappedBean() {
  var id: Long
  var version: Long

  def tableName(): String = {
    this.getClass.getSimpleName.toLowerCase
  }

  override def equals(other: Any): Boolean = other match {
    case bean: DynamoDbMappedBean =>
      this.id == bean.id && this.id == bean.id
    case _ => false
  }

  override def hashCode(): Int = {
    id.hashCode
  }
}
