package my_akka_utils

import akka.actor.{ActorSystem, Props}

import scala.concurrent.duration.DurationInt

object GenerateSocketDataMain {

  def apply(): Unit = {
    val systemActor = ActorSystem("socketGenerator")
    val socketActor = systemActor.actorOf(Props(new GenerateSocketData), "socketActor")
    implicit val defaultDispatcher = systemActor.dispatcher

    import GenerateSocketData._
    systemActor.scheduler.schedule(0 millis, 100 millis){
      socketActor ! GenRecord
    }

    systemActor.scheduler.scheduleOnce(12 seconds){
      systemActor.terminate()
    }

  }

}
