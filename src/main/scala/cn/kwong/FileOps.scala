package cn.kwong

import scala.io.Source

object FileOps {
  def main(args: Array[String]): Unit = {
    val file = Source.fromFile("d:\\mybatis.log")
    for (line <- file.getLines()){
      println(line)
    }
    file.close()
  }
}
