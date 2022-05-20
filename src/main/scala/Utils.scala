import java.io.File

object Utils {

  def listFiles(path: String): Map[String, File] = {
    val content = new File(path)
    val files = content.listFiles().filter(!_.isDirectory)
    Map(files map {
      (file: File) =>
        Tuple2(
          file.getName,
          file
        )
    }: _*)
  }
}
