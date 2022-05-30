package my_akka_utils

import akka.actor.{Actor, ActorLogging}

import java.io.PrintWriter
import java.net.ServerSocket
import scala.util.Random

object GenerateSocketData {
  trait Msg
  object GenRecord extends Msg

  case class Data(
                 id: Int,
                 name: String,
                 qty: Int
                 )
}

class GenerateSocketData extends Actor with ActorLogging {
  import GenerateSocketData._

  val serverSocket = new ServerSocket(9090)
  val socket = serverSocket.accept()
  //val out = new PrintWriter(socket.getOutputStream, true)


  override def preStart(): Unit =
    println("starting")

  //override def postStop(): Unit = socket.close()

  override def receive: Receive = {
    case GenRecord =>
      log.info("generate a record")
      println("generating record")
      val name = Seq("Ann","Seb","luis")
      val rnd = Random
      val data = Data(
        rnd.nextInt(100),
        name(rnd.nextInt(3)),
        rnd.nextInt(1000))
      //out.println(data)
    case _ => log.info("other message")
  }
}
