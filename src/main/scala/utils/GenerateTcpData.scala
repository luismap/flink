package utils

import javassist.bytecode.analysis.Executor
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import sun.util.resources.CalendarData

import java.io.{BufferedReader, OutputStreamWriter, PrintWriter}
import java.net.{ServerSocket, Socket}
import java.time.format.DateTimeFormatter
import java.util.UUID
import scala.collection.mutable
import scala.tools.jline_embedded.internal.InputStreamReader
import scala.util.Random

object GenerateTcpData {
  case class Data(
                   id: UUID,
                   name: String,
                   qty: Int,
                   time: Long,
                   row_num: Int
                 ) {
    def toCsv: String = {
      s"$id $name $qty $time $row_num"
    }
  }

  def apply(): Unit = {

    val server = new ServerSocket(9090)

    val conn = server.accept()
    var cnt = 1

    try {
      while (true){
        val out = new PrintWriter(conn.getOutputStream(), true)
        val names = Seq("ann", "seb", "lui")
        val time = System.currentTimeMillis()
        val data =
          Data(
            UUID.randomUUID(),
            names(Random.nextInt(3)),
            Random.nextInt(1000),
            time,
            cnt
          )
        out.println(data.toCsv)
        Thread.sleep(200)
        cnt += 1
        if (cnt % 10 == 0) Thread.sleep(1000) //for session window
      }
    }
    finally {
      server.close()
    }
  }
}