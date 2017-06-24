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
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{DataType, FloatType}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}
import org.springframework.context.support.FileSystemXmlApplicationContext


object TestJDBC {
  val SUBJECT_SQL = "select * from tb_cas_subject"

  val PAPER_SQL = "select * from tb_exam_paper where id = ?"

  val PAPER_SUBJECT_SQL = "select * from tb_exam_paper_subject where paperId = ? and id = ?"

  val STUDENT_SQL = "select * from tb_exam_temp_seq where paperId =? and batchId = ?"

  val QUESTIONS_SQL = "select * from tb_exam_paper_subject_question where paperId = ? and paperExamId = ?"

  val OBJ_QUE_SQL = "select * from tb_exam_obj_que where paperId = ? and questionId = ?"

  val PAPER_ABSENT_SQL = "select * from tb_exam_student_paper_absent where paperId = ? and paperExamId = ?"
  case class ScoreTbObjScore(paperId:String, paperName:String, paperExamId:String, paperExamName:String,stuId:String,stuSeq:String, schoolId:String, questionId:String, questionOrder:Int, answer:String, ABFlag:Int, isDubious:Int, dubiousType:Int, questionScore:Float, sourceFrom:Int, isChoose:Int, standardAnswer:String)
//  case class Person(height:Float, weight:Float, age:Float)
  def main(args: Array[String]) {
    // 正式【金山云】
    new FileSystemXmlApplicationContext("classpath:test_spring-root-config.xml")
    //本地调试要加下面
//    System.setProperty("hadoop.home.dir", "D:\\source\\hadoop-2.7.1");
//    val conf = new SparkConf().setAppName("calculate").setMaster("local[4]")
    //16个executor,每个能分3个核，2G内存
    val conf = new SparkConf().setAppName("calculate").setMaster("yarn").set("spark.executor.instances","9").setExecutorEnv("spark.executor.cores","3").setExecutorEnv("spark.executor.memory","2G")
    val sc = new SparkContext(conf)
    //sc.addJar("D:\\workspace\\sparkApp\\lib\\mysql-connector-java-5.0.8-bin.jar")
//    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = new HiveContext(sc)
/*********************** test **************************************/
//case class Person(height:Float, weight:Float, age:Float)  注意。在这里定义case class并不能，会编译报错。想想为什么

//    import org.apache.spark.sql.functions._
//    sqlContext.createDataFrame(List(Person(5f,6f,7f),Person(2.5f,6.6f,2.7f),Person(1.55f,8.6f,17f),Person(5f,16f,7.6f),Person(25f,1.6f,0.7f)))
//      .withColumn("operation",col("height") + col("weight") + col("age"))
//      .show()
    /*********************** test **************************************/
 /*   //test1
    sqlContext.sql("use default")
    sqlContext.sql(
      """
        |create table  IF not EXISTS test_spark_sql(
        |id string ,
        |time timestamp
        |)row format delimited fields terminated by '\t'
      """.stripMargin)
    sqlContext.sql("delete from test_spark_sql")
    sqlContext.sql("insert into test_spark_sql values('" + UUID.randomUUID().toString + "','" + new Timestamp(System.currentTimeMillis()) + "')")
    */

      println("上下文初始化成功，开始数据导入。")
    val paperId =  "ecf869e2-e5cd-4b67-9b9c-e387a0267da6"
    val paperExamId = "85550323-1241-4789-bbad-d735a4a02708"



    val sharingRouter = SpringUtil.getSpringBean(null, "shardingRouter").asInstanceOf[SharingRouter]

    import collection.JavaConversions._

    sqlContext.table("rpt_exp_stu_que_score2").rdd.partitions.length

    //科目
    val subjectDF =  SparkJDBCExecutor.executeQuery(sqlContext,new SparkJDBCConfigure("jdbc:mysql://10.254.158.90:3306/zyj_base",SUBJECT_SQL,"admin","JY7wlXUC8rKz","com.mysql.jdbc.Driver",0,0,1000))
    //考试
//    val paperDF = SparkJDBCExecutor.executeQuery(sqlContext,sharingRouter.route(PAPER_SQL, paperId))
    //考试科目
    val paperSubjectDF = SparkJDBCExecutor.executeQuery(sqlContext,sharingRouter.route(PAPER_SUBJECT_SQL, paperId, paperExamId))
    //学生
    val batchIds = SparkJDBCExecutor.executeQuery(sqlContext,sharingRouter.route("select id from tb_exam_temp_seq_batch where paperId = ?", paperId))
      .collect()
    var studentDF =
    if (batchIds.length == 1){
      SparkJDBCExecutor.executeQuery(sqlContext,sharingRouter.route(STUDENT_SQL, paperId, batchIds(0)(0).toString()))
    }else {
      val STUDENT_FIX_SQL = STUDENT_SQL + "and paperExamId = ?"
      batchIds.map(id => sharingRouter.route(STUDENT_SQL, paperId, id(0).toString, paperExamId) )
        .map(SparkJDBCExecutor.executeQuery(sqlContext, _))
        .reduce(_.union(_))
    }
//    studentDF.show(100)
    //题目
    val questionDF = SparkJDBCExecutor.executeQuery(sqlContext,sharingRouter.route(QUESTIONS_SQL, paperId, paperExamId))
    questionDF.cache()
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

   /*//test2
    println("成功导入subject:" + subjectDF.count())
    println("成功导入paperSubject:" + paperSubjectDF.count())
    println("成功导入student:" + studentDF.count())
    println("成功导入question:" + questionDF.count())
    println("成功导入objQue:" + objQueDF.count())
    println("成功导入absent:" + absentDF.count())
    */


    //多选题规则
    val ruleMap = scala.collection.mutable.Map[String, ExamPaperSubjectQuestionRule]()
    questionDF.collect().filter(row => Integer.parseInt(row.getAs("type").toString) == 2)
      .foreach(row => ruleMap += (row.getAs("id").toString -> new ExamPaperSubjectQuestionRule(row.getAs("id").toString, row.getAs("standardAnswer").toString, row.getAs("rules").toString)))

    questionDF.createOrReplaceTempView("tb_exam_paper_subject_question")
    objQueDF.createOrReplaceTempView("tb_exam_obj_que")
    absentDF.createOrReplaceTempView("tb_exam_stu_paper_subject_absent")
    paperSubjectDF.createOrReplaceTempView("tb_exam_paper_subject")
    subjectDF.createOrReplaceTempView("tb_cas_subject")
    studentDF.createOrReplaceTempView("tb_exam_temp_seq")
    import org.apache.spark.sql.functions._

    //计算除了多选题以外的分数
    def score_of_multichoose(questionId:String, answer:String):Float={
      ruleMap(questionId).scoreValueFromAnswer(answer)
    }
    sqlContext.udf.register("score_of_multichoose", score_of_multichoose _)
    val scoreObjScoreDF = sqlContext.sql(
      """
        |SELECT
        |t1.paperId,
        |t1.paperName,
        |t1.paperExamId,
        |t1.paperExamName,
        |t1.stuId,
        |t1.stuSeq,
        |t1.schoolId,
        |t2.subjectId,
        |t1.questionId AS questionId,
        |t1.questionOrder,
        |t2.No AS questionNo,
        |t2.type AS questionType,
        |t2.score as questionScore,
        |t2.difficulty,
        |CASE WHEN (t1.isDubious = 1 AND t1.dubiousType > 0) THEN t1.fixAnswer
        |ELSE t1.answer
        |END AS answer,
        |t1.ABFlag,
        |CASE WHEN t3.dubiousType IS NULL THEN 0 ELSE 1 END AS isDubious,
        |t3.dubiousType,
        |CASE WHEN (t2.type != 4 AND t2.type != 2 AND t1.answer = t2.standardAnswer) THEN t2.score
        |WHEN (t2.type = 2) THEN score_of_multichoose(t1.questionId, t1.answer)
        |ELSE 0
        |END AS stuScore,
        |t3.sourceFrom as sourceScore,
        |t1.isChoose,
        |t2.standardAnswer,
        |concat('c',t2.pos) as pos
        |FROM tb_exam_paper_subject_question t2
        |JOIN tb_exam_obj_que t1
        |ON ( t2.id = t1.questionId)
        |LEFT JOIN tb_exam_stu_paper_subject_absent t3
        |ON (t1.stuId=t3.userId AND t3.status=1)
        |where
        |t1.isChoose > 0
        |and t2.type < 4
        |and t1.status = 1
      """.stripMargin)
    //cache在执行写之后被缓存
    scoreObjScoreDF.cache()

//   scoreObjScoreDF.show(100)

    //为了减少大表的关联次数（学生每题），先将题目、科目、考试信息关联起来

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
      """.stripMargin)
    paperScoreDF.cache()

    //写score_detail表以供报表使用
    scoreObjScoreDF.createOrReplaceTempView("score_tb_exam_obj_score")
    paperScoreDF.createOrReplaceTempView("score_tb_exam_paper_score")

    val stuQueDetail = sqlContext.sql(
      """
        |SELECT
        |rs4.paperId,
        |rs4.paperName,
        |rs4.paperExamId,
        |rs4.paperExamName,
        |rs.stuId,
        |rs.stuSeq,
        |rs4.isAB AS abFlag,
        |rs.isChoose,
        |rs.isDubious,
        |rs.dubiousType,
        |rs.answer,
        |rs.standardAnswer,
        |rs.questionId,
        |rs.questionNo,
        |rs.questionOrder,
        |rs.questionType,
        |rs.questionScore,
        |rs.stuScore,
        |rs.difficulty,
        |rs.pos,
        |rs4.subjectId,
        |rs4.subjectName,
        |rs4.subjectShortName,
        |rs3.stuName,
        |rs3.cityId,
        |rs3.cityName,
        |rs3.areaId,
        |rs3.areaName,
        |rs3.schoolId,
        |rs3.schoolName,
        |rs3.gradeId,
        |rs3.gradeName,
        |rs3.classId,
        |rs3.className,
        |COALESCE (rs3.isRestudy, 1) as isRestudy,
        |COALESCE (rs3.classType, 0) as classType,
        |rs4.objTotal as paperObTotal,
        |rs4.subTotal as paperSubTotal,
        |rs4.paperTotal,
        |NULL AS questionKnowledges,
        |rs4.isComprehensiveSubject
        |FROM score_tb_exam_obj_score rs
        |JOIN score_tb_exam_paper_score rs4
        |ON ( rs.subjectId = rs4.subjectId)
        |JOIN tb_exam_temp_seq rs3
        |ON ( rs.stuId = rs3.userId)
      """.stripMargin)
    val stuQueDetailDF = stuQueDetail.repartition(50, col("stuId")).cache()
    scoreObjScoreDF.count()

//    stuQueDetail.show(100)

    val uuid = udf(()=>UUID.randomUUID().toString)
    //用current_timestamp()替代
//    val currentTime = new Timestamp(System.currentTimeMillis())
    val weijiRule = (3, 1f)
    //实验证明，udf入参不支持Tuple， 返回值不支持Any / AnyVal ，即需要指明返回类型，当需要null值时，可选用Option
    val fixweiji=(dubiousType:Int,score:Float, ruleType:Int,ruleScore:Float)=>{
      if (dubiousType == 4){

        (ruleType,ruleScore) match {
          case (2,_)=>Some(0f)
          case (3,x)=>{
            val tmp = score - Math.abs(x)
            if (tmp > 0f) Some(tmp) else Some(0f)
          }
          case _=>Some(score)
        }
      }else None
    }

    val fixWeijiUDF = udf(fixweiji)

    //存储到文件（这里会有很多分片文件。。。） 使用repartition将其合并
//    objScoreDF.rdd.repartition(1).saveAsTextFile("D:/sparktest/objscore")

    //导出
    ExportObjScore(scoreObjScoreDF)
    ExportStuQueScore(questionDF, stuQueDetailDF)
    stuQueDetailDF.createOrReplaceTempView("score_tb_exam_stu_que_detail")
//    sqlContext.sql("DROP TABLE IF EXISTS test_score_tb_exam_stu_que_detail")
  /*  sqlContext.sql(
      """
        |CREATE TABLE IF NOT EXISTS default.test_score_tb_exam_stu_que_detail (
        |paperId String,
        |paperName   String,
        |paperExamId String,
        |paperExamName   String,
        |stuId  String,
        |stuSeq  String,
        |ABFlag    int,
        |isChoose  int,
        |isDubious int,
        |dubiousType int,
        |stuAnswer   String,
        |standardAnswer   String,
        |questionId String,
        |questionNo String,
        |questionOrder   int,
        |questionType int,
        |score        DECIMAL(12, 4),
        |stuScore     DECIMAL(12, 4),
        |difficulty  String,
        |pos String,
        |subjectId  String,
        |subjectName String,
        |subjectShortName String,
        |userName    String,
        |cityId     String,
        |cityName    String,
        |areaId     String,
        |areaName    String,
        |schoolId   String,
        |schoolName  String,
        |gradeId    String,
        |gradeName   String,
        |classId  String,
        |className String,
        |studentType int,
        |WLType      int,
        |objTotal    DECIMAL(12, 4),
        |subTotal    DECIMAL(12, 4),
        |paperTotal  DECIMAL(12, 4),
        |questionKnowledges String,
        |isComprehensiveSubject INT
        |)row format delimited fields terminated by '\t'
      """.stripMargin)*/
    sqlContext.sql("insert into table default.test_score_tb_exam_stu_que_detail select * from score_tb_exam_stu_que_detail")

    /**
      * 导出到tb_exam_obj_score
      * @param scoreObjScoreDF
      */
    def ExportObjScore(scoreObjScoreDF:DataFrame):Unit ={
      scoreObjScoreDF
        .select("paperId","paperName","paperExamId","paperExamName","stuId","stuSeq","schoolId","questionId","questionOrder","answer","ABFlag","stuScore" )
        .withColumnRenamed("stuScore","questionScore")
        .withColumn("id",  uuid())
        .withColumn("createDate", current_timestamp())
        .withColumn("lastModify", current_timestamp())
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
        ).save()
    }
    /**
      * 将stuQueDetail转置导出到tb_exam_stu_que_score
      */
    def ExportStuQueScore(questionDF:DataFrame, stuQueDetailDF:DataFrame): Unit ={
      //step 1: 先找出tb_exam_paper_subject_question 的pos的Seq
      val quePos  = questionDF
//        .filter("type!=4")
        .select("pos")
        .collect()
        .map(row => row(0))
        .collect{case i: Int => "c" + i}

      val sum3 = udf((a: Any ,b: Any ,c: Any )=>{
        def toFloat(f:Any):Float=f match {
          case None => 0f
          case value:Float =>value
          case _ =>0f
        }
        toFloat(a) + toFloat(b) + toFloat(c)}
      )


      val groupByStu = stuQueDetailDF
        .select("paperId","paperName","paperExamId","paperExamName","stuId","stuSeq","dubiousType","questionType", "pos", "stuScore")
        .groupBy(col("paperId"),col("paperName"),col("paperExamId"),col("paperExamName"),col("stuId"),col("stuSeq"),col("dubiousType"))

      //step 2: 用这个序列来透视pos列
      groupByStu
        .pivot("pos", quePos)
        .sum("stuScore")
        //      .show(100)
        .join(
        groupByStu.agg(sum("stuScore") as "total")
        ,Seq("paperId","paperName","paperExamId","paperExamName","stuId","stuSeq","dubiousType")
        ).join(
        groupByStu
          .pivot("questionType", Seq("1","2","3","4"))
          .sum("stuScore")
          .withColumn("subjectiveTotal",sum3(lit(0f),lit(0f),col("4").cast(FloatType)))
          .withColumn("objectiveTotal", sum3(col("1").cast(FloatType), col("2").cast(FloatType), col("3").cast(FloatType))  )
          .select("paperId","paperName","paperExamId","paperExamName","stuId","stuSeq","dubiousType","subjectiveTotal","objectiveTotal")
        ,Seq("paperId","paperName","paperExamId","paperExamName","stuId","stuSeq","dubiousType")
        )
//        .show(100)
        .withColumn("id",uuid())
        .withColumn("sourceScore",lit(0))
        .withColumn("createtime",current_timestamp())
        //这里有个疑问，客观题总分比违纪扣得分还少时，那么学生就只有0
        .withColumn("fixTotal", fixWeijiUDF(col("dubiousType"), col("total"), lit(weijiRule._1),lit(weijiRule._2)))
        . write.mode(SaveMode.Append).format("jdbc")
        .options(
          Map(
            "url"->"jdbc:mysql://10.254.158.90/zyj_exam_1",
            "dbtable"->  "test_tb_exam_stu_que_score",
            "driver"-> "com.mysql.jdbc.Driver",
            "user"-> "admin",
            "password"-> "JY7wlXUC8rKz")
        ).save()

    }
  }
}
