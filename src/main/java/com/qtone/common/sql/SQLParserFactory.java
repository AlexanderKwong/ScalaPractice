package com.qtone.common.sql;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLDeleteStatement;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.ast.statement.SQLUpdateStatement;
import com.alibaba.druid.sql.dialect.db2.parser.DB2StatementParser;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlASTVisitor;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlOutputVisitor;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlParameterizedOutputVisitor;
import com.alibaba.druid.sql.dialect.oracle.parser.OracleStatementParser;
import com.alibaba.druid.sql.dialect.sqlserver.parser.SQLServerStatementParser;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.alibaba.druid.sql.visitor.SQLASTOutputVisitor;
import com.dangdang.ddframe.rdb.sharding.api.DatabaseType;
import com.dangdang.ddframe.rdb.sharding.exception.SQLParserException;
import com.dangdang.ddframe.rdb.sharding.parser.SQLVisitorRegistry;
import com.dangdang.ddframe.rdb.sharding.parser.result.router.SQLBuilder;
import com.dangdang.ddframe.rdb.sharding.parser.visitor.VisitorLogProxy;
import com.dangdang.ddframe.rdb.sharding.parser.visitor.basic.mysql.MySQLSelectVisitor;

import java.util.Arrays;
import java.util.List;

/**
 * Created by kwong on 2017/5/10.
 */
public class SQLParserFactory {

    private static String parseSQL(String logicSql, List<Object> parameters, DatabaseType databaseType) throws Exception {
        SQLStatement sqlStatement = getSQLStatementParser(databaseType, logicSql).parseStatement();
//        SQLASTOutputVisitor visitor = getSQLVisitor(databaseType, sqlStatement);
//        visitor.setParameters(parameters);
//        sqlStatement.accept(visitor);

        return SQLUtils.toMySqlString(sqlStatement);

    }

    private static SQLStatementParser getSQLStatementParser(DatabaseType code, String sql) {
        switch(code) {
            case MySQL:
                return new MySqlStatementParser(sql);
            case Oracle:
                return new OracleStatementParser(sql);
            case SQLServer:
                return new SQLServerStatementParser(sql);
            case DB2:
                return new DB2StatementParser(sql);
            default:
                throw new UnsupportedOperationException(String.format("Cannot support database type [%d]", code));
        }
    }

    private static SQLASTOutputVisitor getSQLVisitor(DatabaseType databaseType, SQLStatement sqlStatement) throws IllegalAccessException, InstantiationException {
        if(sqlStatement instanceof SQLSelectStatement) {
//            return (SQLASTOutputVisitor) VisitorLogProxy.enhance(SQLVisitorRegistry.getSelectVistor(databaseType));
//            return SQLVisitorRegistry.getSelectVistor(databaseType).newInstance();
            return  new MySqlOutputVisitor(new SQLBuilder());
        } else if(sqlStatement instanceof SQLInsertStatement) {
//            return (SQLASTOutputVisitor)VisitorLogProxy.enhance(SQLVisitorRegistry.getInsertVistor(databaseType));
            return SQLVisitorRegistry.getInsertVistor(databaseType).newInstance();
        } else if(sqlStatement instanceof SQLUpdateStatement) {
//            return (SQLASTOutputVisitor)VisitorLogProxy.enhance(SQLVisitorRegistry.getUpdateVistor(databaseType));
            return SQLVisitorRegistry.getUpdateVistor(databaseType).newInstance();
        } else if(sqlStatement instanceof SQLDeleteStatement) {
//            return (SQLASTOutputVisitor)VisitorLogProxy.enhance(SQLVisitorRegistry.getDeleteVistor(databaseType));
            return SQLVisitorRegistry.getDeleteVistor(databaseType).newInstance();
        } else {
            throw new SQLParserException("Unsupported SQL statement: [%s]", new Object[]{sqlStatement});
        }
    }

    public static void main(String[] args ) throws Exception {
        String sql = "select * from tb_exam_stu_subject_score where paperId = ? and paperExamId = ?";
        System.out.println(parseSQL(sql, Arrays.asList(new Object[]{"ecf869e2-e5cd-4b67-9b9c-e387a0267da6", "85550323-1241-4789-bbad-d735a4a02708"}), DatabaseType.MySQL));
    }
}
