package cn.kwong

import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Created by kwong on 2017/5/10.
  */
object SparkJDBCExecutor {

  def executeQuery(sqlContext:SQLContext, sparkJDBCConfigure:com.qtone.spark.jdbc.SparkJDBCConfigure):DataFrame = {
    sqlContext.read.format("jdbc").options(
      Map(
        "url"->sparkJDBCConfigure.getUrl,
        "dbtable"->  ("(" + sparkJDBCConfigure.getSql + ") as alias"),
        "driver"-> sparkJDBCConfigure.getDriver,
        "user"-> sparkJDBCConfigure.getUser,
        "password"-> sparkJDBCConfigure.getPassword,
        //"partitionColumn"->"day_id",
        "lowerBound"-> sparkJDBCConfigure.getLowerBound.toString,
        "upperBound"->  sparkJDBCConfigure.getUpperBound.toString,
        //"numPartitions"->"2",
        "fetchSize"-> sparkJDBCConfigure.getFetchSize.toString)).load()
  }

}
