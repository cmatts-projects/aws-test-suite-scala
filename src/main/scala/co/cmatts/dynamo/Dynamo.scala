package co.cmatts.dynamo

import co.cmatts.client.Configuration.configureEndPoint
import co.cmatts.dynamo.model.DynamoDbMappedBean
import software.amazon.awssdk.enhanced.dynamodb.{DynamoDbAsyncTable, DynamoDbEnhancedAsyncClient, TableSchema}
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient

object Dynamo {

  private val TABLE_NAME_PREFIX: String = "dynamo.example."
  private var tables: Map[String, DynamoDbAsyncTable[_]] = Map()
  private var enhancedClient: Option[DynamoDbEnhancedAsyncClient] = None
  private var enhancedUnversionedClient: Option[DynamoDbEnhancedAsyncClient] = None

  def getEnhancedDynamoClient: DynamoDbEnhancedAsyncClient = {
    enhancedClient match {
      case Some(enhancedClient) => enhancedClient
      case None =>
        val asyncBuilder = DynamoDbAsyncClient.builder
        configureEndPoint(asyncBuilder)
        val builder: DynamoDbEnhancedAsyncClient.Builder = DynamoDbEnhancedAsyncClient.builder
        builder.dynamoDbClient(asyncBuilder.build)

        enhancedClient = Option(builder.build)
        enhancedClient.get
    }
  }

  def getEnhancedUnversionedDynamoClient: DynamoDbEnhancedAsyncClient = {
    enhancedUnversionedClient match {
      case Some(enhancedUnversionedClient) => enhancedUnversionedClient
      case None =>
        val asyncBuilder = DynamoDbAsyncClient.builder
        configureEndPoint(asyncBuilder)
        val builder: DynamoDbEnhancedAsyncClient.Builder = DynamoDbEnhancedAsyncClient.builder
        builder.dynamoDbClient(asyncBuilder.build)
        builder.extensions()

        enhancedUnversionedClient = Option(builder.build)
        enhancedUnversionedClient.get
    }
  }

  def getDynamoTable[BeanType <: DynamoDbMappedBean](beanClass: Class[BeanType]): DynamoDbAsyncTable[BeanType] = {
    val table: Option[_] = tables.get(beanClass.getSimpleName)
    table match {
      case Some(table) => table.asInstanceOf[DynamoDbAsyncTable[BeanType]]
      case None =>
        val newTable = Option(getDynamoTable(getEnhancedDynamoClient, beanClass))
        tables += (beanClass.getSimpleName -> newTable.get)
        newTable.get
    }
  }

  def getDynamoTable[BeanType <: DynamoDbMappedBean](client: DynamoDbEnhancedAsyncClient, beanClass: Class[BeanType]): DynamoDbAsyncTable[BeanType] = {
    client.table(getTableName(beanClass), TableSchema.fromBean(beanClass))
  }

  private def getTableName[BeanType <: DynamoDbMappedBean](beanClass: Class[BeanType]): String = {
    try {
      val instance: DynamoDbMappedBean = beanClass.getDeclaredConstructor().newInstance()
      val tableName = TABLE_NAME_PREFIX + instance.tableName()
      tableName
    } catch {
      case ex: Exception => throw new IllegalArgumentException("getDynamoTable expects a DynamoDbMappedBean", ex)
    }
  }

}
