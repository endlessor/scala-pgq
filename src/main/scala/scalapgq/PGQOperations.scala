package scalapgq

import org.joda.time.{DateTime,Duration}
import scala.annotation.tailrec

trait PGQOperations {
  type Session
  
  def localTx[A](execution: Session => A): A
  
  def createQueue(queueName: String)(implicit s: Session): Boolean
  def dropQueue(queueName: String, force: Boolean = false)(implicit s: Session): Boolean
  
  def registerConsumer(queueName: String, consumerName: String)(implicit s: Session): Boolean
  def unRegisterConsumer(queueName: String, consumerName: String)(implicit s: Session): Boolean
  
  def nextBatch(queueName: String, consumerName: String)(implicit s: Session): Option[Long]
  def getBatchEvents(batchId: Long)(implicit s: Session): Iterable[Event]
  def finishBatch(batchId: Long)(implicit s: Session): Boolean
  def eventRetry(batchId: Long, eventId: Long, retrySeconds: Duration)(implicit s: Session): Unit
  def eventRetry(batchId: Long, eventId: Long, retryTime: DateTime)(implicit s: Session): Unit
  def eventFailed(batchId: Long, eventId: Long, reason: String)(implicit s: Session): Boolean
  
  def insertEvent(queueName: String, eventType: String, eventData: String)(implicit s: Session): Long = {
    insertEvent(queueName, eventType, eventData, null, null, null, null)
  }
  def insertEvent(queueName: String, eventType: String, eventData: String, extra1: String, extra2: String, extra3: String, extra4: String)(implicit s: Session): Long
  def getQueueInfo()(implicit s: Session): Seq[QueueInfo]
  def getQueueInfo(queueName: String)(implicit s: Session): Option[QueueInfo]
  def getConsumerInfo()(implicit s: Session): Seq[ConsumerInfo]
  def getConsumerInfo(queueName: String)(implicit s: Session): Seq[ConsumerInfo]
  def getConsumerInfo(queueName: String, consumerName: String)(implicit s: Session): Option[ConsumerInfo]
  
}