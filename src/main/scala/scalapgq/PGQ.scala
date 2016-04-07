package scalapgq

import akka.NotUsed
import akka.stream._
import akka.stream.stage._
import akka.stream.scaladsl._
import org.joda.time._
import scala.util.control.NonFatal
import scala.concurrent.duration._

case class Event(
  ev_id: Long,
  ev_time: DateTime,
  ev_txid: Long,
  ev_retry: Option[Int],
  ev_type: Option[String],
  ev_data: Option[String],
  ev_extra1: Option[String],
  ev_extra2: Option[String],
  ev_extra3: Option[String],
  ev_extra4: Option[String]
)

case class QueueInfo(
  queue_name: String,
  queue_ntables: Int,
  queue_cur_table: Int,
  queue_rotation_period: Period,
  queue_switch_time: DateTime,
  queue_external_ticker: Boolean,
  queue_ticker_paused: Boolean,
  queue_ticker_max_count: Int,
  queue_ticker_max_lag: Period,
  queue_ticker_idle_period: Period,
  ticker_lag: Period,
  ev_per_sec: Option[Double],
  ev_new: Long,
  last_tick_id: Long
)

case class ConsumerInfo(
  queue_name: String,
  consumer_name: String,
  lag: Period,
  last_seen: Period,
  last_tick: Long,
  current_batch: Option[Int],
  next_tick: Option[Int],
  pending_events: Long
)

case class BatchInfo(
  queue_name: String,
  consumer_name: String,
  batch_start: DateTime,
  batch_end: DateTime,
  prev_tick_id: Int,
  tick_id: Int,
  lag: Period,
  seq_start: Int,
  seq_end: Int
)

trait PGQ {
  def source(settings: PGQSettings): Source[Event, NotUsed] = {
    Source.fromGraph(new PGQSourceGraphStage(settings))
  }
}

object PGQ extends PGQ

case class PGQSettings(
  val url: String, 
  val user: String, 
  val password: String,
  val queueName: String,
  val consumerName: String,
  val consumerSilencePeriod: FiniteDuration = 500 millis,
  val registerConsumer: Boolean = false
)

class PGQSourceGraphStage(settings: PGQSettings) extends GraphStage[SourceShape[Event]] {
  val out: Outlet[Event] = Outlet("EventsSource")
  override val shape: SourceShape[Event] = SourceShape(out)
  
  val ops = new scalalike.PGQOperationsImpl(settings.url, settings.user, settings.password)
 
  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new TimerGraphStageLogic(shape) {
      if(settings.registerConsumer) {
        ops.localTx { implicit session =>
          ops.registerConsumer(settings.queueName, settings.consumerName)
        }
      }
      
      setHandler(out, new OutHandler {
        override def onPull(): Unit = poll()
      })
      
      override def onTimer(timerKey: Any) = {
        poll()
      }
      
      def poll(): Unit = {
        try {
          //get events in one transaction
          val batchEvents = ops.localTx { implicit session =>
            ops.nextBatch(settings.queueName, settings.consumerName).map { batchId => (batchId, ops.getBatchEvents(batchId)) }
          }
          
          //process them and finish batch in another
          batchEvents match {
            case Some((batchId,events)) =>
              emitMultiple(out, events.iterator, () => ops.localTx { implicit session => ops.finishBatch(batchId);()})
            case None =>
              scheduleOnce(None, settings.consumerSilencePeriod)
          }
          
        } catch {
          case NonFatal(ex) => fail(out, ex)
        }
      }
    }
}

