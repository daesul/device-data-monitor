package de.cboerner.data.device.monitor

import akka.actor.SupervisorStrategy.Restart
import akka.actor.{Actor, ActorLogging, OneForOneStrategy, Props}
import de.cboerner.data.device.monitor.producer.{HeatAlertProducer, RpmAlertProducer}

import scala.concurrent.duration._

object DeviceAlertSupervisor {

  def props(): Props = Props(new DeviceAlertSupervisor)

  val Name = "device-alert-coordinator"

  sealed trait Alert {
    def producerId: Int
  }

  final case class Heat(producerId: Int, heat: Double) extends Alert

  final case class Rpm(producerId: Int, rpm: Int) extends Alert

}

final class DeviceAlertSupervisor extends Actor with ActorLogging {

  import de.cboerner.data.device.monitor.DeviceAlertSupervisor._


  override val supervisorStrategy = OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 30.seconds) {
    case _: Exception => Restart
  }

  private val heatAlertProducer = context.actorOf(HeatAlertProducer.props(), HeatAlertProducer.Name)
  private val rpmAlertProducer = context.actorOf(RpmAlertProducer.props(), RpmAlertProducer.Name)

  context.watch(heatAlertProducer)
  context.watch(rpmAlertProducer)

  private def handleAlert(alert: Alert): Unit = alert match {
    case heat: Heat => heatAlertProducer ! heat
    case rpm: Rpm => rpmAlertProducer ! rpm
  }

  override def receive: Receive = {
    case alert: Alert => handleAlert(alert)
  }
}

