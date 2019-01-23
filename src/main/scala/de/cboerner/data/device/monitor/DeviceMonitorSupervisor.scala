package de.cboerner.data.device.monitor

import akka.Done
import akka.actor.{Actor, ActorLogging, ActorRef, Props}

object DeviceMonitorSupervisor {
  def props(deviceAlertSupervisor: ActorRef): Props = Props(new DeviceMonitorSupervisor(deviceAlertSupervisor))

  val Name = "device-monitor-supervisor"
}

final class DeviceMonitorSupervisor(deviceAlertProducer: ActorRef) extends Actor with ActorLogging {
  import de.cboerner.data.device.monitor.DeviceMonitor._

  private var deviceMonitors: Map[String, ActorRef] = Map.empty


  override def receive: Receive = {
    case dataRecord:DeviceData => createAndForward(dataRecord)
  }

  private def createAndForward(dataRecord: DeviceData) = {
    val monitorName = DeviceMonitor.name(dataRecord.deviceId)
    deviceMonitors.get(monitorName) match {
      case Some(deviceMonitor) => deviceMonitor ! dataRecord
      case None =>
        val newMonitor = context.actorOf(DeviceMonitor.props(deviceAlertProducer), monitorName)
        deviceMonitors += monitorName -> newMonitor
        newMonitor ! dataRecord
    }
    sender() ! Done
  }
}

