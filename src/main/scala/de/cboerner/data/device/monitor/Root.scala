package de.cboerner.data.device.monitor

import akka.actor.SupervisorStrategy.Restart
import akka.actor.{Actor, ActorLogging, OneForOneStrategy, Props, Terminated}

import scala.concurrent.duration._

object Root {
  def props(): Props = Props(new Root)
}

final class Root extends Actor with ActorLogging{

  override val supervisorStrategy = OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 30.seconds) {
    case _: Exception => Restart
  }

  val deviceAlertSupervisor = context.actorOf(DeviceAlertSupervisor.props(), DeviceAlertSupervisor.Name)
  val deviceMonitorSupervisor = context.actorOf(DeviceMonitorSupervisor.props(deviceAlertSupervisor), DeviceMonitorSupervisor.Name)
  val deviceDataConsumer = context.actorOf(DeviceDataConsumer.props(deviceMonitorSupervisor), DeviceDataConsumer.Name)

  context.watch(deviceAlertSupervisor)
  context.watch(deviceMonitorSupervisor)
  context.watch(deviceDataConsumer)

  log.info("Started")

  override def receive: Receive = {
    case Terminated(actor) =>
      log.info("Shutting down system, because {} was not able to restart", actor.path.name)
  }
}
