package utils

import java.io.PrintWriter
import java.net.ServerSocket
import java.util.UUID
import scala.io.Source
import scala.util.Random

object GenerateFromFileTcpData {


  def apply(file: String, port: Int, interval: Int = 500): Unit = {

    val server = new ServerSocket(port)

    val conn = server.accept()
    val lines = Source.fromFile(file).getLines()
    try {
        val out = new PrintWriter(conn.getOutputStream(), true)

         lines.foreach { line =>
         out.println(line)
         Thread.sleep( interval)
        }

    }
    finally {
      server.close()
    }
  }
}