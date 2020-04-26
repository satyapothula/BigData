package ca.mcit.bigdata.bixipro
import java.io.{BufferedWriter, File, FileWriter}

import scala.io.Source
object Sourcefiles extends App {

  val si = Source.fromURL("https://api-core.bixi.com/gbfs/en/station_information.json")
  val s = si.mkString

  def siwriteFile(filename: String, s: String): Unit = {
    val file = new File(filename)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(s)
    bw.close()
  }
  siwriteFile("/home/bd-user/Downloads/Bixi/Autodownload/station_information.json", s)

  val sys = Source.fromURL("https://api-core.bixi.com/gbfs/en/system_information.json")
  val sy = sys.mkString

  def sywriteFile(filename: String, sy: String): Unit = {
    val file = new File(filename)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(sy)
    bw.close()
  }

  sywriteFile("/home/bd-user/Downloads/Bixi/Autodownload/system_information.json", sy)

  println("Source files auto downloaded into LFS")
}



