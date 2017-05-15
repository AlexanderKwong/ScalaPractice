package cn.kwong.datasource

import com.qtone.common.sharding.ShardingDataSourceHolder
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils._
import org.apache.spark.sql.{ DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.execution.datasources.jdbc._
import org.apache.spark.sql.sources.BaseRelation


class ShardingJdbcRelationProvider extends JdbcRelationProvider{

  override def shortName(): String = "shardingJDBC"

  override def createRelation(
                               sqlContext: SQLContext,
                               mode: SaveMode,
                               parameters: Map[String, String],
                               df: DataFrame): BaseRelation = {
    val jdbcOptions = new JDBCOptions(parameters)
    val url = jdbcOptions.url
    val table = jdbcOptions.table
    val createTableOptions = jdbcOptions.createTableOptions
    val isTruncate = jdbcOptions.isTruncate

//    val conn = JdbcUtils.createConnectionFactory(jdbcOptions)()
    val conn = new ShardingDataSourceHolder().getConnection
    try {
      val tableExists = JdbcUtils.tableExists(conn, url, table)
      if (tableExists) {
        mode match {
          case SaveMode.Overwrite =>
            if (isTruncate && isCascadingTruncateTable(url) == Some(false)) {
              // In this case, we should truncate table and then load.
              truncateTable(conn, table)
              saveTable(df, url, table, jdbcOptions)
            } else {
              // Otherwise, do not truncate the table, instead drop and recreate it
              dropTable(conn, table)
              createTable(df.schema, url, table, createTableOptions, conn)
              saveTable(df, url, table, jdbcOptions)
            }

          case SaveMode.Append =>
            saveTable(df, url, table, jdbcOptions)

          case SaveMode.ErrorIfExists =>
            throw new RuntimeException(
              s"Table or view '$table' already exists. SaveMode: ErrorIfExists.")

          case SaveMode.Ignore =>
          // With `SaveMode.Ignore` mode, if table already exists, the save operation is expected
          // to not save the contents of the DataFrame and to not change the existing data.
          // Therefore, it is okay to do nothing here and then just return the relation below.
        }
      } else {
        createTable(df.schema, url, table, createTableOptions, conn)
        saveTable(df, url, table, jdbcOptions)
      }
    } finally {
      conn.close()
    }

    createRelation(sqlContext, parameters)
  }
}
