package de.cboerner.data.device.monitor

import akka.actor.{Actor, ActorLogging, Props}

object DeviceDataParser {

  val Name = "device-alert-parser"

  def props(): Props = Props(new DeviceDataParser)

  final case class RawDeviceData(value: String)

}

final class DeviceDataParser extends Actor with ActorLogging {

  import de.cboerner.data.device.monitor.DeviceDataParser._
  import io.circe._
  import io.circe.parser._
  import de.cboerner.data.device.monitor.DeviceMonitor._

  override def receive: Receive = {
    case rawDeviceData: RawDeviceData =>
      parse(rawDeviceData.value) match {
        case Left(failure) => throw new IllegalArgumentException(failure)
        case Right(json) =>
          val cursor = json.hcursor
          val deviceId = cursor.downField("deviceId").as[Int].getOrElse(0)
          val temperature = cursor.downField("temperature").as[Double].getOrElse(0.0)
          val rpm = cursor.downField("rpm").as[Int].getOrElse(0)
          val timestamp = cursor.downField("timestamp").as[Long].getOrElse(0L)
          sender() ! DeviceData(deviceId, temperature, rpm, timestamp)
      }

  }
}

