package de.cboerner.data.device.monitor

import java.time.Instant

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import de.cboerner.data.device.monitor.DeviceAlertSupervisor.{Heat, Rpm}

import scala.concurrent.duration._

object DeviceMonitor {

  def name(id: Int): String = s"device-monitor-$id"

  def props(deviceAlertProducer: ActorRef): Props = Props(new DeviceMonitor(deviceAlertProducer))

  final case class DeviceData(deviceId: Int, temperature: Double, rpm: Int, timestamp: Long = Instant.now().toEpochMilli)

  final object Reset

  final case class State(
                          recordsReceived: Seq[DeviceData] = Seq.empty,
                          avgTemperature: Double = 0.0,
                          avgRotationPerMinute: Double = 0.0) {

    def +(dataRecord: DeviceData): State = {
      val newRecordsReceived = this.recordsReceived :+ dataRecord
      val avgTemp = newRecordsReceived.map(_.temperature).sum / newRecordsReceived.size
      val avgRpm = newRecordsReceived.map(_.rpm).sum / newRecordsReceived.size
      copy(recordsReceived = newRecordsReceived, avgTemp, avgRpm)
    }

    def overheated: Boolean = avgTemperature > 65

    def rotationCritical: Boolean = avgRotationPerMinute > 6500
  }


}

final class DeviceMonitor(deviceAlertProducer: ActorRef) extends Actor with ActorLogging {

  import de.cboerner.data.device.monitor.DeviceMonitor._
  import context.dispatcher

  context.system.scheduler.schedule(2.minutes, 2.minutes, self, Reset)

  private var state = State()

  override def receive: Receive = {
    case dataRecord: DeviceData =>
      state = state + dataRecord
      if (state.overheated) {
        log.info("Creating heat alert for: {}", dataRecord.deviceId)
        deviceAlertProducer ! Heat(dataRecord.deviceId, dataRecord.temperature, Instant.now().toEpochMilli)
      }

      if (state.rotationCritical) {
        log.info("Creating rpm alert for: {}", dataRecord.deviceId)
        deviceAlertProducer ! Rpm(dataRecord.deviceId, dataRecord.rpm,  Instant.now().toEpochMilli)
      }
    case Reset => state = State()
  }
}


