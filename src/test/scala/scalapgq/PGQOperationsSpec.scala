package scalapgq.scalalike

import scalapgq._
import org.scalatest._
import org.scalatest.concurrent._
import scala.concurrent.duration._
import scalikejdbc._

class PGQOperationsSpec extends FlatSpec with Matchers with BeforeAndAfter with Eventually with IntegrationPatience {
  
//  ConnectionPool.singleton("jdbc:postgresql://127.0.0.1/instafin", "instafin", "instafin")
  GlobalSettings.loggingSQLAndTime = LoggingSQLAndTimeSettings(enabled = true, singleLineMode = true)
  
  val PostgresUser = "PostgresUser"
  val PostgresUrl = s"jdbc:postgresql://127.0.0.1/$PostgresUser"
  val PostgresPassword = "PostgresPassword"
  
  val pgq = new PGQOperationsImpl(PostgresUrl, PostgresUser, PostgresPassword)
  val queueName = "test_queue"
  val consumerName = "test_consumer"
  
  final def getNextNonEmptyEvents(queueName: String, consumerName: String)(implicit session: pgq.Session): Iterable[Event] = {
    eventually {
      val batch_id = pgq.nextBatch(queueName, consumerName)
  	  batch_id match {
  	    case Some(id) => {
  	      val events = pgq.getBatchEvents(id) 
  	      pgq.finishBatch(id)
  	      events match {
  	        case Nil => getNextNonEmptyEvents(queueName, consumerName)
  	        case s => s 
  	      }
  	    }
  	    case None => throw new Exception("No batchId")
  	  }
    }
  }
    
  before {
    pgq.localTx{implicit session =>
      if(pgq.getQueueInfo(queueName).isDefined) {
        if(pgq.getConsumerInfo(queueName, consumerName).isDefined) {
          pgq.unRegisterConsumer(queueName, consumerName)
        }
        pgq.dropQueue(queueName)
      }
    }
  }
  "PGQ" should 
    "create_queue and drop queue" in {
      pgq.localTx{implicit session =>
        pgq.createQueue(queueName) shouldEqual true
        pgq.dropQueue(queueName) shouldEqual true
      }
    }
    it should "register and unregister consumer" in {
      pgq.localTx{implicit session =>
        pgq.createQueue(queueName) shouldEqual true 
        pgq.registerConsumer(queueName, consumerName) shouldEqual true
        pgq.unRegisterConsumer(queueName, consumerName) shouldEqual true
      }
    }
    it should "insert and consume event" in {
      val consumerName = "test_consumer"
      pgq.localTx{implicit session =>
        pgq.createQueue(queueName) shouldEqual true 
        pgq.registerConsumer(queueName, consumerName) shouldEqual true
      }
      
      pgq.localTx{implicit session =>
        pgq.insertEvent(queueName, "type", "data")
      }
      
      pgq.localTx{implicit session =>
        getNextNonEmptyEvents(queueName, consumerName) should not be empty
      }
    }
    
    
    it should "rollback inserted events" in {
      pgq.localTx{implicit session =>
        pgq.createQueue(queueName) shouldEqual true 
        pgq.registerConsumer(queueName, consumerName) shouldEqual true
      }

	    a[Exception] should be thrownBy {
	      pgq.localTx { implicit session =>
	        pgq.insertEvent(queueName, "type", "data1")
	        throw new Exception()
	      }
	    }
	    
	    pgq.localTx { implicit session =>
	        pgq.insertEvent(queueName, "type", "data2")
	      }
      
      val events = pgq.localTx { implicit session =>
        getNextNonEmptyEvents(queueName, consumerName)
      }.map(_.ev_data) 
      
      events should not contain (Some("data1"))
      events should contain (Some("data2"))  
    }
}