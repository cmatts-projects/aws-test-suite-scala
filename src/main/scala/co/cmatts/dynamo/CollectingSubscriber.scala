package co.cmatts.dynamo

import org.reactivestreams.{Subscriber, Subscription}

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit.MILLISECONDS
import scala.collection.mutable.ListBuffer

class CollectingSubscriber[T] extends Subscriber[T] {
  private val TIMEOUT_MILLIS: Long = 5000L

  private val latch: CountDownLatch = new CountDownLatch(1)
  val collectedItems: ListBuffer[T] = ListBuffer()
  var error: Option[Throwable] = None
  var isCompleted: Boolean = false

  @Override
  def onSubscribe(subscription: Subscription): Unit = {
    subscription.request(Long.MaxValue)
  }

  @Override
  def onNext(t: T): Unit = {
    collectedItems.addOne(t)
  }

  @Override
  def onError(throwable: Throwable): Unit = {
    error = Some(throwable)
    latch.countDown()
  }

  @Override
  def onComplete(): Unit = {
    this.isCompleted = true
    this.latch.countDown()
  }

  def waitForCompletion(): Unit = {
    try {
      this.latch.await(TIMEOUT_MILLIS, MILLISECONDS)
    } catch {
      case e: InterruptedException => throw new RuntimeException(e)
    }
  }

}