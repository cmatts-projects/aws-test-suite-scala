package co.cmatts.dynamo

import co.cmatts.dynamo.Dynamo.{getDynamoTable, getEnhancedDynamoClient, getEnhancedUnversionedDynamoClient}
import co.cmatts.dynamo.model.{DynamoDbMappedBean, Fact, Person, Siblings}
import software.amazon.awssdk.core.async.SdkPublisher
import software.amazon.awssdk.enhanced.dynamodb.{DynamoDbAsyncIndex, DynamoDbAsyncTable, Key}
import software.amazon.awssdk.enhanced.dynamodb.model.QueryConditional.keyEqualTo
import software.amazon.awssdk.enhanced.dynamodb.model._

import java.util.concurrent.CompletableFuture
import scala.jdk.CollectionConverters._

class DynamoRepository {

  private val FATHER_INDEX: String = "fatherIndex"
  private val MOTHER_INDEX: String = "motherIndex"
  private val PERSON_INDEX: String = "personIndex"
  private val BATCH_SIZE: Int = 25

  def findPerson(id: Integer): Option[Person] = {
    if (id == null) {
      return None
    }

    val idKey: Key = Key.builder().partitionValue(id).build()
    val result: CompletableFuture[Person] = getDynamoTable(classOf[Person])
      .asInstanceOf[DynamoDbAsyncTable[Person]]
      .getItem(idKey)

    Option(result.join())
  }

  def findPersonByFather(id: Integer): Seq[Person] = {
    findEntitiesByIndex(id, classOf[Person], FATHER_INDEX)
      .asInstanceOf[Seq[Person]]
  }

  def findPersonByMother(id: Integer): Seq[Person] = {
    findEntitiesByIndex(id, classOf[Person], MOTHER_INDEX)
      .asInstanceOf[Seq[Person]]
  }

  def findFacts(id: Integer): Seq[Fact] = {
    findEntitiesByIndex(id, classOf[Fact], PERSON_INDEX)
      .asInstanceOf[Seq[Fact]]
  }

  private def findEntitiesByIndex[T <: DynamoDbMappedBean](id: Integer, beanClass: Class[T], index: String): Seq[T] = {
    if (id == null) {
      return Seq.empty
    }

    val parentIndex: DynamoDbAsyncIndex[T] = getDynamoTable(beanClass).index(index)
    val request: QueryEnhancedRequest = QueryEnhancedRequest.builder()
      .consistentRead(false)
      .queryConditional(keyEqualTo(Key.builder().partitionValue(id).build()))
      .build()
    val publisher: SdkPublisher[Page[T]] = parentIndex.query(request)

    collectFromPublisher(publisher)
  }

  private def collectFromPublisher[T](publisher: SdkPublisher[Page[T]]): Seq[T] = {
    val subscriber: CollectingSubscriber[Page[T]] = new CollectingSubscriber[Page[T]]()
    publisher.subscribe(subscriber)
    subscriber.waitForCompletion()

    if (subscriber.error.isDefined) {
      throw new IllegalStateException("DynamoDb query failed with an error", subscriber.error.get)
    }
    if (!subscriber.isCompleted) {
      throw new IllegalStateException("DynamoDb query failed to get all content")
    }

    subscriber.collectedItems.map(_.items().asScala.toSeq).flatten.toSeq
  }

  def findSiblings(id: Integer): Siblings = {
    val p: Option[Person] = findPerson(id)
    if (p.isEmpty) {
      return new Siblings()
    }
    val person: Person = p.get

    val allSiblings: Set[Person] = Set(findPersonByFather(person.getFatherId()) ++ findPersonByMother(person.getMotherId()))

    new Siblings(person, allSiblings.toSeq, extractParents(allSiblings))
  }

  private def extractParents(allSiblings: Set[Person]): Seq[Person] = {
    allSiblings
      .flatMap(s => Seq(s.getFatherId(), s.getMotherId()))
      .filter(p => p != null)
      .toSeq
      .distinct
      .sorted
      .map(findPerson)
      .map(_.get)
  }

  def findPeople(): Seq[Person] = {
    val request: ScanEnhancedRequest = ScanEnhancedRequest.builder()
      .consistentRead(true)
      .build()

    val publisher: PagePublisher[Person] = getDynamoTable(classOf[Person]).asInstanceOf[DynamoDbAsyncTable[Person]].scan(request)

    collectFromPublisher(publisher).sortBy(_.getName)
  }

  def load(dataLists: Seq[_ <: DynamoDbMappedBean]*): Unit = {
    val allData: Seq[DynamoDbMappedBean] = dataLists.flatMap(_.toSeq)
    allData.grouped(BATCH_SIZE).foreach(batch => loadBatch(batch))
  }

  private def loadBatch(data: Seq[DynamoDbMappedBean]): Unit = {
    val entities: Map[Class[DynamoDbMappedBean], Seq[DynamoDbMappedBean]] = data
      .groupBy(g => g.getClass.asInstanceOf[Class[DynamoDbMappedBean]])

    val batchWrites: Seq[WriteBatch] = entities
      .map(e => loadBatchForEntity(e._2, e._1))
      .toSeq

    val batchRequest: BatchWriteItemEnhancedRequest = BatchWriteItemEnhancedRequest
      .builder.writeBatches(batchWrites
      .asInstanceOf[WriteBatch]).build
    val asyncResult: CompletableFuture[BatchWriteResult] = getEnhancedUnversionedDynamoClient
      .batchWriteItem(batchRequest)

    val result: BatchWriteResult = asyncResult.join()

    entities.keySet.foreach(beanClass => validateBatchWrite(result, beanClass))
  }

  private def validateBatchWrite[T <: DynamoDbMappedBean](result: BatchWriteResult, beanClass: Class[T]): Unit = {
    val table: DynamoDbAsyncTable[T] = getDynamoTable(getEnhancedUnversionedDynamoClient, beanClass)
    if (result.unprocessedPutItemsForTable(table).size() > 0) {
      throw new IllegalStateException("DynamoDb query failed to write all content")
    }
  }

  private def loadBatchForEntity[T <: DynamoDbMappedBean](entities: Seq[T], beanClass: Class[T]): WriteBatch = {
    val table: DynamoDbAsyncTable[T] = getDynamoTable(getEnhancedUnversionedDynamoClient, beanClass)

    val batch: WriteBatch.Builder[T] = WriteBatch.builder(beanClass).mappedTableResource(table)
    entities.foreach(e => batch.addPutItem(e))
    batch.build()
  }

  def updateEntities(entities: Seq[_ <: DynamoDbMappedBean]): Unit = {
    val updateRequestBuilder: TransactWriteItemsEnhancedRequest.Builder = TransactWriteItemsEnhancedRequest.builder()
    entities.foreach(e => addUpdateForEntity(updateRequestBuilder, e))

    val update: CompletableFuture[Void] = getEnhancedDynamoClient
      .transactWriteItems(updateRequestBuilder.build())
    update.join()
    entities.foreach(e => e.version = e.version + 1)
  }

  private def addUpdateForEntity[T <: DynamoDbMappedBean](builder: TransactWriteItemsEnhancedRequest.Builder, entity: T): Unit = {
    val beanClass: Class[T] = entity.getClass.asInstanceOf[Class[T]]
    val updateRequest: TransactUpdateItemEnhancedRequest[T] = TransactUpdateItemEnhancedRequest.builder(beanClass).item(entity).build()
    builder.addUpdateItem(getDynamoTable(beanClass), updateRequest)
  }
}
