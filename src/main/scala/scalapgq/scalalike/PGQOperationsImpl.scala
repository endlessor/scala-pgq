package scalapgq.scalalike

import scalapgq._
import org.joda.time.{DateTime, Duration, Period}

class PGQOperationsImpl(url: String, user: String, password: String) extends PGQOperations {
  import scalikejdbc._
  
  type Session = DBSession
  
  ConnectionPool.add('named, url, user, password)
  
  override def localTx[A](execution: DBSession => A) = NamedDB('named).localTx(execution)
  
  override def createQueue(queueName: String)(implicit s: Session): Boolean = {
    sql"select pgq.create_queue(${queueName})".map(_.boolean(1)).single.apply().getOrElse(false)
  }
  override def dropQueue(queueName: String, force: Boolean = false)(implicit s: Session): Boolean = {
    sql"select pgq.drop_queue(${queueName}, ${force})".map(_.boolean(1)).single.apply().getOrElse(false)
  }

  override def registerConsumer(queueName: String, consumerName: String)(implicit s: Session): Boolean = {
    sql"select pgq.register_consumer(${queueName}, ${consumerName})".map(_.boolean(1)).single.apply().getOrElse(false)
  }
  override def unRegisterConsumer(queueName: String, consumerName: String)(implicit s: Session): Boolean = {
    sql"select pgq.unregister_consumer(${queueName}, ${consumerName})".map(_.boolean(1)).single.apply().getOrElse(false)
  }
  
  override def nextBatch(queueName: String, consumerName: String)(implicit s: Session): Option[Long] = {
    sql"select next_batch from pgq.next_batch(${queueName}, ${consumerName})"
      .map(rs => rs.longOpt("next_batch"))
      .single
      .apply()
      .flatten
  }
  def getBatchEvents(batchId: Long)(implicit s: Session): Iterable[Event] = {
    sql"select * from pgq.get_batch_events(${batchId})"
      .map(rs => new Event(
        rs.long("ev_id"),
        rs.jodaDateTime("ev_time"),
        rs.long("ev_txid"),
        rs.intOpt("ev_retry"),
        rs.stringOpt("ev_type"),
        rs.stringOpt("ev_data"),
        rs.stringOpt("ev_extra1"),
        rs.stringOpt("ev_extra2"),
        rs.stringOpt("ev_extra3"),
        rs.stringOpt("ev_extra4")))
      .list
      .apply()
  }
  def finishBatch(batchId: Long)(implicit s: Session): Boolean = {
    sql"select pgq.finish_batch(${batchId})".map(_.boolean(1)).single.apply().getOrElse(false)
  }
  
  override def eventRetry(batchId: Long, eventId: Long, retryDuration: Duration)(implicit s: Session) = {
    sql"select pgq.event_retry(${batchId}, ${eventId}, ${retryDuration.getStandardSeconds().toInt})".execute.apply()
  }

  override def eventRetry(batchId: Long, eventId: Long, retryTime: DateTime)(implicit s: Session) = {
    sql"select pgq.event_retry(${batchId}, ${eventId}, ${retryTime})".execute.apply()
  }

  override def eventFailed(batchId: Long, eventId: Long, reason: String)(implicit s: Session) = {
    sql"select pgq.event_failed(${batchId}, ${eventId}, ${reason})".execute.apply()
  }
  
  def getQueueInfo()(implicit s: Session): Seq[QueueInfo] = {
    sql"select * from pgq.get_queue_info()"
      .map(rs => new QueueInfo(
        rs.string("queue_name"),
        rs.int("queue_ntables"),
        rs.int("queue_cur_table"),
        rs.get("queue_rotation_period"),
        rs.jodaDateTime("queue_switch_time"),
        rs.boolean("queue_external_ticker"),
        rs.boolean("queue_ticker_paused"),
        rs.int("queue_ticker_max_count"),
        rs.get("queue_ticker_max_lag"),
        rs.get("queue_ticker_idle_period"),
        rs.get("ticker_lag"),
        rs.doubleOpt("ev_per_sec"),
        rs.long("ev_new"),
        rs.long("last_tick_id")
      ))
      .list
      .apply()
  }
  def getQueueInfo(queueName: String)(implicit s: Session): Option[QueueInfo] = {
    sql"select * from pgq.get_queue_info(${queueName})"
      .map(rs => new QueueInfo(
        rs.string("queue_name"),
        rs.int("queue_ntables"),
        rs.int("queue_cur_table"),
        rs.get("queue_rotation_period"),
        rs.jodaDateTime("queue_switch_time"),
        rs.boolean("queue_external_ticker"),
        rs.boolean("queue_ticker_paused"),
        rs.int("queue_ticker_max_count"),
        rs.get("queue_ticker_max_lag"),
        rs.get("queue_ticker_idle_period"),
        rs.get("ticker_lag"),
        rs.doubleOpt("ev_per_sec"),
        rs.long("ev_new"),
        rs.long("last_tick_id")
      ))
      .single
      .apply()
  }
  
  def getConsumerInfo()(implicit s: Session): Seq[ConsumerInfo] = {
    sql"select * from pgq.get_consumer_info()"
      .map(rs => new ConsumerInfo(
        rs.string("queue_name"),
        rs.string("consumer_name"),
        rs.get("lag"),
        rs.get("last_seen"),
        rs.long("last_tick"),
        rs.intOpt("current_batch"),
        rs.intOpt("next_tick"),
        rs.long("pending_events")
      ))
      .list
      .apply()
  }
  def getConsumerInfo(queueName: String)(implicit s: Session): Seq[ConsumerInfo] = {
    sql"select * from pgq.get_consumer_info(${queueName})"
      .map(rs => new ConsumerInfo(
        rs.string("queue_name"),
        rs.string("consumer_name"),
        rs.get("lag"),
        rs.get("last_seen"),
        rs.long("last_tick"),
        rs.intOpt("current_batch"),
        rs.intOpt("next_tick"),
        rs.long("pending_events")
      ))
      .list
      .apply()
  }
  def getConsumerInfo(queueName: String, consumerName: String)(implicit s: Session): Option[ConsumerInfo] = {
    sql"select * from pgq.get_consumer_info(${queueName}, ${consumerName})"
      .map(rs => new ConsumerInfo(
        rs.string("queue_name"),
        rs.string("consumer_name"),
        rs.get("lag"),
        rs.get("last_seen"),
        rs.long("last_tick"),
        rs.intOpt("current_batch"),
        rs.intOpt("next_tick"),
        rs.long("pending_events")
      ))
      .first
      .apply()
  }
  
  def insertEvent(queueName: String, eventType: String, eventData: String, extra1: String = null, extra2: String = null, extra3: String = null, extra4: String = null)(implicit s: Session): Long = {
    sql"select pgq.insert_event(${queueName}, ${eventType} , ${eventData}, ${extra1}, ${extra2}, ${extra3}, ${extra4})".map(_.long(1)).single.apply().get
  }
  
  implicit val array: TypeBinder[Period] = {
    val mathContext = new java.math.MathContext(4)
    TypeBinder(_ getObject _)(_ getObject _).map{
      case ts: org.postgresql.util.PGInterval => {
        val years = ts.getYears
        val months = ts.getMonths
        val days = ts.getDays
        val hours = ts.getHours
        val mins = ts.getMinutes
        val seconds = Math.floor(ts.getSeconds).asInstanceOf[Int]
        val secondsAsBigDecimal = new java.math.BigDecimal(ts.getSeconds,mathContext)
        val millis = secondsAsBigDecimal.subtract(new java.math.BigDecimal(seconds)).multiply(new java.math.BigDecimal(1000)).intValue
  
        new Period(years,months, 0, days, hours, mins, seconds,millis).normalizedStandard
      }
    }
  }
}