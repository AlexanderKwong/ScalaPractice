package cn.kwong

import org.springframework.context.support.FileSystemXmlApplicationContext

/**
  * Created by kwong on 2017/5/24.
  */
object Calculate {

  def main(args: Array[String]): Unit = {

//    if (check(args)){
//      println(usage)
//      return
//    }

    def run:(Array[String]=>Int)={
      case Array("0",reDo, paperId, paperExamId) => {
        new FileSystemXmlApplicationContext("classpath:test_spring-root-config.xml")
        println("开始客观题统分")
        val weijiRule = (3, 1f)
        (new ObjQuesCalcExecutor(paperId, paperExamId, java.lang.Boolean.valueOf(reDo))).start()(weijiRule)

      }
      case Array("1",x,y,_) => {
        new FileSystemXmlApplicationContext("classpath:test_spring-root-config.xml")
        println("开始主观题统分")
        //(new SubjectiveQuesCalcExecutor(x,y)).execute()
        0
      }
      case _ => {
        println(usage)
        0
      }
    }

    run(args) match {
      case 0 => println("执行成功")
      case 1 => throw new IllegalArgumentException()
      case 2 => throw new RuntimeException("")
      case _ =>
    }

  }

  def check:(Array[String] =>Boolean) = (args:Array[String] )=>{
    false
  }

  val usage =
    """
      |计算分数入参指定：
      |    args[0] -> 客观题：0 ，主观题：1
      |    args[1] -> 重新生成，是：1 ，否：0
      |    args[2] -> 考试Id(paperId)
      |    args[3] -> 考试科目Id(paperExamId)
    """.stripMargin
}
