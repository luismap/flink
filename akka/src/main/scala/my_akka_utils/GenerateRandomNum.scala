package my_akka_utils

import akka.actor.{ActorSystem, Props}

import java.io.File
import scala.concurrent.duration.DurationInt


class GenerateRandomNum(file: File) {

  val actorSystem = ActorSystem("actorSystem")

  val randomGenActor = actorSystem.actorOf(Props(RandomNumberGen(file)))
  import RandomNumberGen._
  implicit val context = actorSystem.dispatcher

  actorSystem.scheduler.schedule(0 millis, 10 millis){
    randomGenActor ! GenRandomInt
  }

  actorSystem.scheduler.scheduleOnce(10 seconds) {
    actorSystem.terminate()
  }
}

object GenerateRandomNum {
  def apply(file: File): GenerateRandomNum = new GenerateRandomNum(file)
}
