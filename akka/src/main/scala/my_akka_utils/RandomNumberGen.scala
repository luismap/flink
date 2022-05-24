package my_akka_utils

import akka.actor.{Actor, ActorLogging}

import java.io.{File, PrintWriter}
import scala.util.Random


object RandomNumberGen {
  trait RNGMsg

  object GenRandomInt extends RNGMsg

  def apply(file: File): RandomNumberGen =
    new RandomNumberGen(file)
}

class RandomNumberGen(file: File) extends Actor with ActorLogging {

  import RandomNumberGen._

  var fileWriter: PrintWriter = null

  override def preStart(): Unit = {
    fileWriter = new PrintWriter(file)
  }
  override def postStop(): Unit = {
    fileWriter.close
  }

  override def receive: Receive = {
    case GenRandomInt =>
      log.info(s"[writing a random number to ${file.getPath}")
      fileWriter.write( Random.nextInt(2000) + "\n")
      fileWriter.flush()
  }
}
