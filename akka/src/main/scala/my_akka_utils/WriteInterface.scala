package my_akka_utils

import java.io.File
import scala.io.BufferedSource

trait WriteInterface {
  val file: File
  val bufferedSource = toFile(file)

  def toFile(file: File): BufferedSource

  def toStdout: Unit
}
