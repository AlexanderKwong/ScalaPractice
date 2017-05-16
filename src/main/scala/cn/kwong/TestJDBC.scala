package cn.kwong

import java.sql.Timestamp
import java.util
import java.util.{Properties, UUID}
import java.util.stream.Collectors

import com.alibaba.druid.pool.DruidAbstractDataSource
import com.dangdang.ddframe.rdb.sharding.api.rule.DataSourceRule
import com.qtone.common.sharding.SharingRouter
import com.qtone.common.spring.SpringUtil
import com.qtone.entity.ExamPaperSubjectQuestionRule
import com.qtone.spark.jdbc.SparkJDBCConfigure
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}
import org.springframework.context.support.FileSystemXmlApplicationContext

import scala.reflect.internal.util.TableDef.Column

object TestJDBC {

  case class ScoreTbObjScore(paperId:String, paperName:String, paperExamId:String, paperExamName:String,stuId:String,stuSeq:String, schoolId:String, questionId:String, questionOrder:Int, answer:String, ABFlag:Int, isDubious:Int, dubiousType:Int, questionScore:Float, sourceFrom:Int, isChoose:Int, standardAnswer:String)

  def main(args: Array[String]) {
    // 正式【金山云】
    new FileSystemXmlApplicationContext("classpath:test_spring-root-config.xml")
    System.setProperty("hadoop.home.dir", "D:\\source\\hadoop-2.7.1");
    val conf = new SparkConf().setAppName("mysql").setMaster("local[4]")
    val sc = new SparkContext(conf)
    //sc.addJar("D:\\workspace\\sparkApp\\lib\\mysql-connector-java-5.0.8-bin.jar")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val paperId =  "ecf869e2-e5cd-4b67-9b9c-e387a0267da6"
    val paperExamId = "85550323-1241-4789-bbad-d735a4a02708"

    val SUBJECT_SQL = "select * from tb_cas_subject"

    val PAPER_SQL = "select * from tb_exam_paper where id = ?"

    val PAPER_SUBJECT_SQL = "select * from tb_exam_paper_subject where paperId = ? and id = ?"

    val STUDENT_SQL = "select * from tb_exam_temp_seq where paperId =? and paperExamId = ? and batchId = ?"

    val QUESTIONS_SQL = "select * from tb_exam_paper_subject_question where paperId = ? and paperExamId = ?"

    val OBJ_QUE_SQL = "select * from tb_exam_obj_que where paperId = ? and questionId = ?"

    val PAPER_ABSENT_SQL = "select * from tb_exam_student_paper_absent where paperId = ? and paperExamId = ?"

    val sharingRouter = SpringUtil.getSpringBean(null, "shardingRouter").asInstanceOf[SharingRouter]

    import collection.JavaConversions._
/*
    //考试
    val paperDF = SparkJDBCExecutor.executeQuery(sqlContext,sharingRouter.route(PAPER_SQL, paperId))
    //考试科目
    val paperSubjectDF = SparkJDBCExecutor.executeQuery(sqlContext,sharingRouter.route(PAPER_SUBJECT_SQL, paperId, paperExamId))
    //学生
    val studentDF = SparkJDBCExecutor.executeQuery(sqlContext,sharingRouter.route(STUDENT_SQL, paperId, paperExamId))*/
    //题目
    val questionDF = SparkJDBCExecutor.executeQuery(sqlContext,sharingRouter.route(QUESTIONS_SQL, paperId, paperExamId))
    //客观题
    val objQueDF  = questionDF
      .filter(row =>  Integer.parseInt(row.getAs("type").toString).toInt != 4)
      .select("id")
      .collect()
      //      .foreach(println)
      .map(a => sharingRouter.route(OBJ_QUE_SQL, paperId, a(0).toString))
      .map(SparkJDBCExecutor.executeQuery(sqlContext, _))
      .reduce(_.union(_))
    //缺考
    val absentDF = SparkJDBCExecutor.executeQuery(sqlContext, sharingRouter.route(PAPER_ABSENT_SQL, paperId, paperExamId))

    //多选题规则
    val ruleMap = scala.collection.mutable.Map[String, ExamPaperSubjectQuestionRule]()
    questionDF.collect().filter(row => Integer.parseInt(row.getAs("type").toString) == 2)
      .foreach(row => ruleMap += (row.getAs("id").toString -> new ExamPaperSubjectQuestionRule(row.getAs("id").toString, row.getAs("standardAnswer").toString, row.getAs("rules").toString)))

    questionDF.createOrReplaceTempView("tb_exam_paper_subject_question")
    objQueDF.createOrReplaceTempView("tb_exam_obj_que")
    absentDF.createOrReplaceTempView("tb_exam_stu_paper_subject_absent")
    //计算除了多选题以外的分数
    def score_of_multichoose(questionId:String, answer:String):Float={
      ruleMap(questionId).scoreValueFromAnswer(answer)
    }
    sqlContext.udf.register("score_of_multichoose", score_of_multichoose _)
    val scoreObjScoreDF = sqlContext.sql("SELECT t1.paperId, t1.paperName, t1.paperExamId, t1.paperExamName, t1.stuId, t1.stuSeq, t1.schoolId, t1.questionId AS questionId, t1.questionOrder, CASE WHEN (t1.isDubious = 1 AND t1.dubiousType > 0) THEN t1.fixAnswer ELSE t1.answer END AS answer, t1.ABFlag, CASE WHEN t3.dubiousType IS NULL THEN 0 ELSE 1 END AS isDubious, t3.dubiousType, CASE WHEN (t2.type != 4 AND t2.type != 2 AND t1.answer = t2.standardAnswer) THEN t2.score WHEN (t2.type = 2) THEN score_of_multichoose(t1.questionId,t1.answer) ELSE 0 END AS questionScore, t3.sourceFrom as sourceScore, t1.isChoose, t2.standardAnswer ,concat('c',t2.pos) as pos FROM tb_exam_paper_subject_question t2 JOIN tb_exam_obj_que t1 ON (t1.paperId = t2.paperId AND t1.paperExamId = t2.paperExamId AND t2.id = t1.questionId) LEFT JOIN tb_exam_stu_paper_subject_absent t3 ON (t1.paperId = t3.paperId AND t1.paperExamId=t3.paperExamId AND t1.stuId=t3.userId AND t3.status=1) where t1.paperId = 'ecf869e2-e5cd-4b67-9b9c-e387a0267da6' and t1.paperExamId = '85550323-1241-4789-bbad-d735a4a02708' and t1.isChoose > 0 and t2.type < 4 and t1.status = 1")

//   scoreObjScoreDF.show(100)
    //用到自勉到函数lit()
    import org.apache.spark.sql.functions._

    //为了减少大表的关联次数（学生每题），先将题目、科目、考试信息关联起来

    //写score_detail表以供报表使用
  /*  scoreObjScoreDF.createOrReplaceTempView("score_tb_exam_obj_score")
    studentDF.createOrReplaceTempView("tb_exam_issue_student")

    //得到试卷的基本信息，客观题，主观题，总分分数，其中综合科将包含它自身，以及N个拆开的子科目
    val paperScoreDF = sqlContext.sql(
      """
        |SELECT
        |t3.paperId,
        |t3.paperName,
        |t3.Id AS paperExamId,
        |t3.paperExamName,
        |t2.id AS subjectId,
        |t2.subjectName,
        |t2.shortName as subjectShortName,
        |ROUND (SUM (CASE WHEN t1.TYPE != 4 THEN t1.score ELSE 0 END), 2)
        |objTotal,
        |ROUND (SUM (CASE WHEN t1.TYPE = 4 THEN t1.score ELSE 0 END), 2)
        |subTotal,
        |ROUND (SUM (t1.score), 2) paperTotal,
        |COALESCE (t3.isAB, 0) AS isAB,
        |t3.isComprehensive as isComprehensiveSubject
        |FROM tb_exam_paper_subject_question t1
        |JOIN tb_exam_paper_subject t3
        |ON (t1.paperId = t3.paperId AND t1.paperExamId = t3.id)
        |JOIN
        |tb_cas_subject t2
        |ON (t1.subjectId = t2.id)
        |WHERE t1.questionOrder>0
        |GROUP BY
        |t3.paperId,
        |t3.paperName,
        |t3.id,
        |t3.paperExamName,
        |t3.isAB,
        |t3.isComprehensive,
        |t2.Id,
        |t2.subjectName,
        |t2.shortName
        |union all
        |SELECT
        |t2.paperId,
        |t2.paperName,
        |t2.Id AS paperExamId,
        |t2.paperExamName,
        |t2.id AS subjectId,
        |t2.paperExamName AS subjectName,
        |'ZH' AS subjectShortName,
        |ROUND (SUM (CASE WHEN t1.TYPE != 4 THEN t1.score ELSE 0 END), 2) AS objTotal,
        |ROUND (SUM (CASE WHEN t1.TYPE = 4 THEN t1.score ELSE 0 END), 2) AS subTotal,
        |ROUND (SUM (t1.score), 2) paperTotal,
        |COALESCE (t2.isAB, 0) AS isAB,
        |0 AS isComprehensiveSubject
        |FROM tb_exam_paper_subject_question t1
        |JOIN tb_exam_paper_subject t2
        |ON (t1.paperId = t2.paperId AND t1.paperExamId = t2.id)
        |WHERE t2.isComprehensive=1 and t1.questionOrder>0
        |GROUP BY t2.paperId,
        |t2.paperName,
        |t2.id,
        |t2.paperExamName,
        |t2.isAB,
        |t2.isComprehensive
      """.stripMargin
    )*/




    val uuid = udf(()=>UUID.randomUUID().toString)
    val currentTime = new Timestamp(System.currentTimeMillis())

    //写入tb_exam_obj_score
   /* scoreObjScoreDF
        .select("paperId","paperName","paperExamId","paperExamName","stuId","stuSeq","schoolId","questionId","questionOrder","answer","ABFlag","questionScore")
      .withColumn("id",  uuid())
      .withColumn("createDate", lit(currentTime))
      .withColumn("lastModify", lit(currentTime))
      .withColumn("sourceScore", lit(0))
      .withColumn("status", lit(1))
      .withColumn("lastModifier", lit("ZYJ-V3.0"))
      .withColumn("creator", lit("ZYJ-V3.0"))
      . write.mode(SaveMode.Append).format("jdbc")
      .options(
        Map(
          "url"->"jdbc:mysql://10.254.158.90/zyj_exam_1",
          "dbtable"->  "test_tb_exam_obj_score",
          "driver"-> "com.mysql.jdbc.Driver",
          "user"-> "admin",
          "password"-> "JY7wlXUC8rKz")
      ).save()*/
    //存储到文件（这里会有很多分片文件。。。） 使用repartition将其合并
//    objScoreDF.rdd.repartition(1).saveAsTextFile("D:/sparktest/objscore")

    //写入tb_exam_stu_que_score，按学生进行group by 并 行专列
    //step 1: 先找出tb_exam_paper_subject_question 的pos的Seq
    val objQuePos  = questionDF
      .filter("type!=4")
      .select("pos")
      .collect()
      .map(row => row(0))
      .collect{case i: Int => "c" + i}
    //step 2: 用这个序列来透视pos列
    val groupByStu = scoreObjScoreDF
      .select("paperId","paperName","paperExamId","paperExamName","stuId","stuSeq","dubiousType","pos", "questionScore")
      .groupBy(col("paperId"),col("paperName"),col("paperExamId"),col("paperExamName"),col("stuId"),col("stuSeq"),col("dubiousType"))
    groupByStu
      .pivot("pos", objQuePos)
      .sum("questionScore")
//      .show(100)
        .join(
      groupByStu.agg(sum("questionScore") as "objectiveTotal")
      ,Seq("paperId","paperName","paperExamId","paperExamName","stuId","stuSeq","dubiousType")
    )
      .show(100)
//      .withColumn("id", uuid())

    /**
      * 将stuQueDetail转置导出到tb_exam_stu_que_score
      * @param stuQueDetail
      */
    def stuQueScore(stuQueDetail:DataFrame): Unit ={

    }
  }
}
