package cn.kwong

import com.qtone.common.sharding.SharingRouter
import com.qtone.common.spring.SpringUtil
import com.qtone.spark.jdbc.SparkJDBCConfigure
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.mutable
//import cn.kwong.ObjQuesCalcExecutor._
import cn.kwong.common.BusinessUDF._
import cn.kwong.common.GeneralUDF._
import com.qtone.entity.ExamPaperSubjectQuestionRule
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.FloatType

/**
  * Created by kwong on 2017/6/25.
  * 优化策略：
  * （1）按维度建模，可分为 题目维度，考生维度，考生答题事实
  * （2）考虑到题目维度表体积不会大到哪儿去（一场考试），使用先join来完善描述，再broadcast到所有的executor
  * （3）考生与考生答题表在进来时进行repartition使DataFrame(RDD)按学生的Id来分区，因为统分的定语是“学生的”，主要计算学生的题目-科目-总分，方便随后的join，以及combine不产生shuffle
  * （4）要被广播的在算子使用的数据结构要统一使用Kryo进行序列化
  * （5）存入HDFS中时，使用parquet进行存储
  */
class ObjQueCalcExecutor(paperId:String, paperExamId:String, redo: Boolean)  extends Serializable{
  import cn.kwong.ObjQueCalcExecutor._
  /**
    * sparkSession上下文
    */
  //TODO 正式使用时应该读取配置文件（本地/zk）
  val sparkSession = SparkSession
    .builder()
    .appName("objScore-calculate")
    .master("yarn")
    .config("spark.executor.instances", "15")
    .config("spark.executor.cores","3")
    .config("spark.executor.memory","2G")
    //.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") //在新的api中如何conf.registerKryoClasses()
    //      .master("local[4]")
    .enableHiveSupport()
    .getOrCreate()
  /**
    * 分表分库路由
    */
  val sharingRouter = SpringUtil.getSpringBean(null, "shardingRouter").asInstanceOf[SharingRouter]

  def start:(()=>((_,_))=>Int)={
    //科目
    val subjectDF =  SparkJDBCExecutor.executeQuery(sparkSession,new SparkJDBCConfigure("jdbc:mysql://10.254.158.90:3306/zyj_base",SUBJECT_SQL,"admin","JY7wlXUC8rKz","com.mysql.jdbc.Driver",0,0,1000))
    //考试
    //    val paperDF = SparkJDBCExecutor.executeQuery(sqlContext,sharingRouter.route(PAPER_SQL, paperId))
    //考试科目
//    val paperSubjectDF = SparkJDBCExecutor.executeQuery(sparkSession,sharingRouter.route(PAPER_SUBJECT_SQL, paperId, paperExamId))
    //学生
    val batchIds = SparkJDBCExecutor.executeQuery(sparkSession,sharingRouter.route("select id from tb_exam_temp_seq_batch where paperId = ?", paperId))
      .collect()
    var studentDF =
      if (batchIds.length == 1){//一次导入
        SparkJDBCExecutor.executeQuery(sparkSession,sharingRouter.route(STUDENT_SQL, paperId, batchIds(0)(0).toString()))
      }else {//多次导入
      val STUDENT_FIX_SQL = STUDENT_SQL + "and paperExamId = ?"
        batchIds.map(id => sharingRouter.route(STUDENT_SQL, paperId, id(0).toString, paperExamId) )
          .map(SparkJDBCExecutor.executeQuery(sparkSession, _))
          .reduce(_.union(_))
      }
    //todo 按学生分区.repartition(50, col("stuId")) ，为的是与学生题目join而不发生shuffle
        .repartition(50, col("userId"))
    //    studentDF.show(100)
    //题目
    //todo 题目维度应该用考试信息和其他的科目信息进行详细描述，作为一张维度表，并broadcast到所有的executor，用于mapJoin
    val questionDF = SparkJDBCExecutor.executeQuery(sparkSession,sharingRouter.route(QUESTIONS_SQL, paperId, paperExamId)).cache()
    val fieldNames = questionDF.schema.fieldNames
    val questions = questionDF.collect()
    val questionMapBroadcast = sparkSession.sparkContext.broadcast(questions.map(row => {
      val questionRowMap = new mutable.HashMap[String, Any]()
      for(  i <- 0 to fieldNames.length ){
        questionRowMap += ((fieldNames(i), row(i)))
      }
      //id , Map
      (row(0), questionRowMap)
    }).toMap)

    //题目信息--导出到hive
    println("step2:题目信息--导出到hive")
//    questionDF.write.insertInto("test_tb_exam_paper_subject_question")
    //多选题规则
    //todo ruleMap 同理，应该被broadcast；并且使用Kryo进行序列化
    val ruleMap = scala.collection.mutable.Map[String, ExamPaperSubjectQuestionRule]()
    questions.filter(row => Integer.parseInt(row.getAs("type").toString) == 2)
      .foreach(row => ruleMap += (row.getAs("id").toString -> new ExamPaperSubjectQuestionRule(row.getAs("id").toString, row.getAs("standardAnswer").toString, row.getAs("rules").toString)))

    val ruleMapBroadcast = sparkSession.sparkContext.broadcast(ruleMap)
    //客观题
    val objQueDF  = questionDF
      .filter(row =>  Integer.parseInt(row.getAs("type").toString).toInt != 4)
      .select("id")
      .collect()
      //      .foreach(println)
      .map(a => sharingRouter.route(OBJ_QUE_SQL, paperId, a(0).toString))
      .map(SparkJDBCExecutor.executeQuery(sparkSession, _))
      .reduce(_ union _)
    //todo 按学生进行分区 .repartition(50,col("stuId"))
      .repartition(50,col("stuId"))
    //缺考
    val absentDF = SparkJDBCExecutor.executeQuery(sparkSession, sharingRouter.route(PAPER_ABSENT_SQL, paperId, paperExamId))

    questionDF.createOrReplaceTempView("tb_exam_paper_subject_question")
    objQueDF.createOrReplaceTempView("tb_exam_obj_que")
    absentDF.createOrReplaceTempView("tb_exam_stu_paper_subject_absent")
//    paperSubjectDF.createOrReplaceTempView("tb_exam_paper_subject")
    subjectDF.createOrReplaceTempView("tb_cas_subject")
    studentDF.createOrReplaceTempView("tb_exam_temp_seq")

    //计算除了多选题以外的分数
    import sparkSession._
    def score_of_multichoose(questionId:String, answer:String):Float={
      ruleMapBroadcast.value.apply(questionId).scoreValueFromAnswer(answer)
    }
    udf.register("score_of_multichoose", score_of_multichoose _)

    ()=>{
      val scoreObjScoreDF = sql(SCORE_OBJ_SCORE_SQL).cache()
      //客观题分数--导出到Mysql
      ExportObjScore(scoreObjScoreDF)

      val paperScoreDF = sql(PAPER_SCORE_SQL).cache()
      //科目信息--导出到hive
      println("step2:科目信息--导出到hive")
      paperScoreDF.write.insertInto("test_score_tb_exam_paper_score")

      scoreObjScoreDF.createOrReplaceTempView("score_tb_exam_obj_score")
      paperScoreDF.createOrReplaceTempView("score_tb_exam_paper_score")

      (weijiRule:(_,_))=>{
        val stuQueDetailDF = sql(STU_QUE_DETAIL_SQL).repartition(50, col("stuId")).cache()
        //题目细节--导出到hive
        println("step3:题目细节--导出到hive")
        stuQueDetailDF.write.insertInto("test_score_tb_exam_stu_que_detail")
        //题目查询--导出到jdbc
        ExportStuQueScore(questionDF, stuQueDetailDF, weijiRule)

        0
      }

    }
  }
}
object ObjQueCalcExecutor extends Serializable{

  private val SUBJECT_SQL = "select * from tb_cas_subject"
  //todo 下面三条导入语句，可以汇成一句？
  private val PAPER_SQL = "select * from tb_exam_paper where id = ?"

  private val PAPER_SUBJECT_SQL = "select * from tb_exam_paper_subject where paperId = ? and id = ?"

//  private val QUESTIONS_SQL = "select * from tb_exam_paper_subject_question where paperId = ? and paperExamId = ?"

  private val OBJ_QUE_SQL = "select * from tb_exam_obj_que where paperId = ? and questionId = ?"

  private val STUDENT_SQL = "select * from tb_exam_temp_seq where paperId =? and batchId = ?"

  private val PAPER_ABSENT_SQL = "select * from tb_exam_student_paper_absent where paperId = ? and paperExamId = ?"

  private val SCORE_OBJ_SCORE_SQL = """
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
                                    """.stripMargin

  private val PAPER_SCORE_SQL = """
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

  private val STU_QUE_DETAIL_SQL = """
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
                                   """.stripMargin

  private def ExportObjScore(scoreObjScoreDF:DataFrame):Unit ={
    scoreObjScoreDF
      .select("paperId","paperName","paperExamId","paperExamName","stuId","stuSeq","schoolId","questionId","questionOrder","answer","ABFlag","stuScore" )
      .withColumnRenamed("stuScore","questionScore")
      .withColumn("id",  uuidUDF())
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

  private def ExportStuQueScore(questionDF:DataFrame, stuQueDetailDF:DataFrame, weijiRule:(_,_)): Unit ={
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

//todo 尽量避免使用 groubyKey,使用aggresiateBy 和 combineBy 和 reduceBy 代替
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
      .withColumn("id",uuidUDF())
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

  private val QUESTIONS_SQL =
    """
      |select
      |psq.id as question_id,
      |psq.parentId as question_parentId,
      |psq.paperId as question_paperId,
      |psq.paperName as question_paperName,
      |psq.paperExamName as question_paperExamName,
      |psq.paperExamId as question_paperExamId,
      |psq.img as question_img,
      |psq.score as question_score,
      |psq.type as question_type,
      |psq.No as question_No,
      |psq.questionFstNum as question_questionFstNum,
      |psq.hasQuestionFst as question_hasQuestionFst,
      |psq.questionOrder as question_questionOrder,
      |psq.answerCount as question_answerCount,
      |psq.standardAnswer as question_standardAnswer,
      |psq.answerRemak as question_answerRemak,
      |psq.rules as question_rules,
      |psq.difficulty as question_difficulty,
      |psq.subjectId as question_subjectId,
      |psq.scoreType as question_scoreType,
      |psq.scoreSeq as question_scoreSeq,
      |psq.scoreStep as question_scoreStep,
      |psq.pos as question_pos,
      |psq.isOpt as question_isOpt,
      |psq.groupTitle as question_groupTitle,
      |psq.ruleId as question_ruleId,
      |ps.subjectId as paperSubject_subjectId,
      |ps.subjectName as paperSubject_subjectName,
      |ps.startTime as paperSubject_startTime,
      |ps.endTime as paperSubject_endTime,
      |ps.score as paperSubject_score,
      |ps.shouldExamCount as paperSubject_shouldExamCount,
      |ps.batchId as paperSubject_batchId,
      |ps.difficulty as paperSubject_difficulty,
      |ps.markingScope as paperSubject_markingScope,
      |ps.examStatus as paperSubject_examStatus,
      |ps.objectiveStatus as paperSubject_objectiveStatus,
      |ps.subjectiveStatus as paperSubject_subjectiveStatus,
      |ps.mergeStatus as paperSubject_mergeStatus,
      |ps.templateName as paperSubject_templateName,
      |ps.templateId as paperSubject_templateId,
      |ps.anserCardCount as paperSubject_anserCardCount,
      |ps.imgJson as paperSubject_imgJson,
      |ps.cuserId as paperSubject_cuserId,
      |ps.ComprehensiveDetail as paperSubject_ComprehensiveDetail,
      |ps.isImport as paperSubject_isImport,
      |ps.importType as paperSubject_importType,
      |ps.isAb as paperSubject_isAb,
      |ps.isComprehensive as paperSubject_isComprehensive,
      |ps.isOptional as paperSubject_isOptional,
      |ps.tempSeqCount as paperSubject_tempSeqCount,
      |ps.ComplexDetailName as paperSubject_ComplexDetailName,
      |ps.templateVersion as paperSubject_templateVersion,
      |ps.templatePath as paperSubject_templatePath,
      |ps.isCustom as paperSubject_isCustom,
      |ps.seq as paperSubject_seq,
      |ps.pos as paperSubject_pos,
      |ps.ruleConfigType as paperSubject_ruleConfigType,
      |ps.ruleConfigScore as paperSubject_ruleConfigScore,
      |p.paperStatus as paper_paperStatus,
      |p.gradeId as paper_gradeId,
      |p.gradename as paper_gradename,
      |p.paperType as paper_paperType,
      |p.scope as paper_scope,
      |p.seqStatus as paper_seqStatus,
      |p.batchType as paper_batchType,
      |p.cschoolId as paper_cschoolId,
      |p.cschoolName as paper_cschoolName,
      |p.provId as paper_provId,
      |p.provName as paper_provName,
      |p.cityId as paper_cityId,
      |p.cityName as paper_cityName,
      |p.areaId as paper_areaId,
      |p.areaName as paper_areaName,
      |p.status as paper_status,
      |p.reportStatus as paper_reportStatus,
      |p.graphReportStatus as paper_graphReportStatus,
      |p.fileUrl as paper_fileUrl,
      |p.useAccountTemp as paper_useAccountTemp,
      |p.year as paper_year,
      |p.seq as paper_seq,
      |p.paperTime as paper_paperTime,
      |p.startTime as paper_startTime,
      |p.endTime as paper_endTime
      |from
      |tb_exam_paper p
      |join tb_exam_paper_subject ps on (ps.paperId = p.id)
      |join tb_exam_paper_subject_question psq on (psq.paperExamId = ps.id)
      |where p.id=? and ps.id =?
      |and p.status = 1 and ps.status = 1 and psq.status = 1
    """.stripMargin
}