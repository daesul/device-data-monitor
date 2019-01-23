package de.cboerner.data.device.monitor.producer

import akka.actor.{Actor, ActorLogging, Props}
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.Producer
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import de.cboerner.data.device.monitor.DeviceAlertSupervisor.Heat
import org.apache.kafka.clients.producer.ProducerRecord
import io.circe.generic.auto._
import io.circe.syntax._
import org.apache.kafka.common.serialization.StringSerializer

object HeatAlertProducer {

  def props(): Props = Props(new HeatAlertProducer)

  val Name = "heat-alert-producer"
}

final class HeatAlertProducer extends Actor with ActorLogging{
  implicit val mat = ActorMaterializer()

  val config = context.system.settings.config.getConfig("akka.kafka.producer")
  val producerSettings = ProducerSettings(config, new StringSerializer, new StringSerializer)
    .withBootstrapServers("")

  override def receive: Receive = {
    case heat:Heat =>
      Source
        .single(heat)
        .map(h => h.asJson)
        .map{value =>
          log.info(s"Value is: $value")
          new ProducerRecord[String, String]("heat-alert", value.noSpaces)
        }
        .runWith(Producer.plainSink(producerSettings))
  }
}


