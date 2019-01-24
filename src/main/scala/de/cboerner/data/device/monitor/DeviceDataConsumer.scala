package de.cboerner.data.device.monitor

import akka.Done
import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.kafka.{CommitterSettings, ConsumerSettings, Subscriptions}
import akka.kafka.scaladsl.{Committer, Consumer}
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.pattern.ask
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Keep, Sink}
import akka.util.Timeout
import de.cboerner.data.device.monitor.DeviceMonitor.DeviceData
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer

import scala.concurrent.duration._

object DeviceDataConsumer {
  def props(deviceMonitorSupervisor: ActorRef): Props = Props(new DeviceDataConsumer(deviceMonitorSupervisor))

  val Name = "device-data-consumer"
}

final class DeviceDataConsumer(deviceMonitorSupervisor: ActorRef) extends Actor with ActorLogging {

  import de.cboerner.data.device.monitor.DeviceDataParser._

  implicit val timeout: Timeout = 2.seconds

  implicit val mat = ActorMaterializer()

  import context.dispatcher

  val deviceAlertparser = context.system.actorOf(DeviceDataParser.props(), DeviceDataParser.Name)
  val config = context.system.settings.config.getConfig("akka.kafka.consumer")
  val consumerSettings =
    ConsumerSettings(config, new StringDeserializer, new StringDeserializer)
      .withBootstrapServers("172.29.15.246:9092")
      .withGroupId("group1")
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  val committerSettings = CommitterSettings(context.system)

  log.info("Start consuming topic")

  Consumer
    .committableSource(consumerSettings, Subscriptions.topics("device-data"))
    .mapAsync(1) { msg =>
      log.info("Receiving raw message: {}", msg.record.value())
      (deviceAlertparser ? RawDeviceData(msg.record.value()))
        .mapTo[DeviceData]
        .map(deviceData => (msg.committableOffset, deviceData))
    }
    .mapAsync(1) {
      offsetAndData => (deviceMonitorSupervisor ? offsetAndData._2)
        .mapTo[Done]
        .map(_ => offsetAndData._1)
    }
    .via(Committer.flow(committerSettings))
    .toMat(Sink.seq)(Keep.both)
    .mapMaterializedValue(DrainingControl.apply)
    .run()


  override def receive: Receive = Actor.emptyBehavior
}

