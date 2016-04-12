package scalapgq

import org.scalatest._
import org.scalatest.concurrent._
import akka.actor._
import akka.stream._
import akka.testkit._
import akka.stream.testkit.scaladsl._
import scala.concurrent.duration._
import scalikejdbc._

class PGQConsumerSpec extends TestKit(ActorSystem("IntegrationSpec")) with WordSpecLike with PGQSpec with Matchers with BeforeAndAfter {
  
  implicit val mat = ActorMaterializer()(system)
  implicit val ec = system.dispatcher
  
  val ops = new scalalike.PGQOperationsImpl(PostgresUrl, PostgresUser, PostgresPassword)
    
  def withQueue(testCode: String => Any) {
    import java.util.UUID.randomUUID
    val queueName = s"test_queue_${randomUUID()}"
    ops.localTx { implicit s =>  ops.createQueue(queueName) }
    try{
      testCode(queueName)
    } finally {
      ops.localTx { implicit s =>  ops.dropQueue(queueName, force = true) }
    }
  }
  
  def withQueueWithNElements(n: Int)(testCode: (String , String) => Any) {
    withQueue { queueName =>
      import java.util.UUID.randomUUID
      val consumerName = s"test_consumer_${randomUUID()}"
      ops.localTx { implicit session => ops.registerConsumer(queueName, consumerName) }
      
      ops.localTx { implicit session =>
        (1 to n) foreach { i =>
          ops.insertEvent(queueName, "eventType", s"eventData_$i")
        }
      }
      testCode(queueName, consumerName)
    }
  }
  
  "PGQ streams" should {
//    "Try to consume events but error" in {
//      PGQ.source(PGQSettings("jdbc:postgresql://127.0.0.1/db", "wronguser", "wronpass", "queue", "consumer"))
//        .runWith(TestSink.probe[Event])
//        .request(1)
//        .expectError()
//    }
    
    "Try to consume events from empty queue" in withQueue { queueName => 
      val consumerName = "test_consumer"
      PGQ.source(PGQSettings(PostgresUrl, PostgresUser, PostgresPassword, queueName, consumerName, registerConsumer = true))
        .runWith(TestSink.probe[Event])
        .request(1)
        .expectNoMsg()
    }
    
    "Consume events already in queue" in withQueueWithNElements(5) { (queueName, consumerName) => 
      PGQ.source(PGQSettings(PostgresUrl, PostgresUser, PostgresPassword, queueName, consumerName))
        .runWith(TestSink.probe[Event])
        .request(5)
        .receiveWithin(6 seconds, 5)
    }
    
    "Consume new events" in withQueueWithNElements(0) { (queueName, consumerName) =>
      PGQ.source(PGQSettings(PostgresUrl, PostgresUser, PostgresPassword, queueName, consumerName))
        .runWith(TestSink.probe[Event])
        .request(2)
        .expectNoMsg()
    }
  }
  
  
  
  
  
}