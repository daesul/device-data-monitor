package de.cboerner.data.device.monitor

import akka.actor.ActorSystem

object Main {

  def main(args: Array[String]): Unit = {
    val system = ActorSystem("device-monitor")
    val root = system.actorOf(Root.props(), "root")
  }
}
