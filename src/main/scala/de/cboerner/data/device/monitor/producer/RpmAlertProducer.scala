package de.cboerner.data.device.monitor.producer

import akka.actor.{Actor, ActorLogging, Props}
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.Producer
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import de.cboerner.data.device.monitor.DeviceAlertSupervisor.{Heat, Rpm}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import io.circe.generic.auto._
import io.circe.syntax._

object RpmAlertProducer {

  def props(): Props = Props(new RpmAlertProducer)

  val Name = "rpm-alert-producer"
}


final class RpmAlertProducer extends Actor with ActorLogging{
  implicit val mat = ActorMaterializer()

  val config = context.system.settings.config.getConfig("akka.kafka.producer")
  val producerSettings = ProducerSettings(config, new StringSerializer, new StringSerializer)
    .withBootstrapServers("172.29.15.246:9092")

  override def receive: Receive = {
    case rpm:Rpm =>
      Source
        .single(rpm)
        .map(r => r.asJson)
        .map{value =>
          log.info(s"Value is: $value")
          new ProducerRecord[String, String]("rpm-alert", value.noSpaces)
        }
        .runWith(Producer.plainSink(producerSettings))
  }
}



