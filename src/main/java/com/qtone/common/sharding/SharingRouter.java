package com.qtone.common.sharding;

import com.alibaba.druid.pool.DruidAbstractDataSource;
import com.alibaba.druid.pool.DruidPooledPreparedStatement;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLDeleteStatement;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.ast.statement.SQLUpdateStatement;
import com.alibaba.druid.sql.dialect.db2.parser.DB2StatementParser;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.dialect.oracle.parser.OracleStatementParser;
import com.alibaba.druid.sql.dialect.sqlserver.parser.SQLServerStatementParser;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.alibaba.druid.sql.visitor.SQLASTOutputVisitor;
import com.alibaba.druid.util.StringUtils;
import com.dangdang.ddframe.rdb.sharding.api.DatabaseType;
import com.dangdang.ddframe.rdb.sharding.api.config.ShardingConfiguration;
import com.dangdang.ddframe.rdb.sharding.api.config.ShardingConfigurationConstant;
import com.dangdang.ddframe.rdb.sharding.api.rule.DataSourceRule;
import com.dangdang.ddframe.rdb.sharding.api.rule.ShardingRule;
import com.dangdang.ddframe.rdb.sharding.exception.SQLParserException;
import com.dangdang.ddframe.rdb.sharding.exception.ShardingJdbcException;
import com.dangdang.ddframe.rdb.sharding.executor.ExecutorEngine;
import com.dangdang.ddframe.rdb.sharding.jdbc.ShardingConnection;
import com.dangdang.ddframe.rdb.sharding.jdbc.ShardingContext;
import com.dangdang.ddframe.rdb.sharding.metrics.MetricsContext;
import com.dangdang.ddframe.rdb.sharding.metrics.ThreadLocalObjectContainer;
import com.dangdang.ddframe.rdb.sharding.parser.SQLParserFactory;
import com.dangdang.ddframe.rdb.sharding.parser.SQLVisitorRegistry;
import com.dangdang.ddframe.rdb.sharding.parser.visitor.VisitorLogProxy;
import com.dangdang.ddframe.rdb.sharding.router.SQLExecutionUnit;
import com.dangdang.ddframe.rdb.sharding.router.SQLRouteEngine;
import com.dangdang.ddframe.rdb.sharding.router.SQLRouteResult;
import com.google.common.base.Preconditions;
import com.qtone.spark.jdbc.SparkJDBCConfigure;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

/**
 * Created by kwong on 2017/5/8.
 */
public class SharingRouter {

    private final ThreadLocalObjectContainer threadLocalObjectContainer;
    private final ShardingContext context;
    private final DataSourceRule dataSourceRule;

    public SharingRouter(ShardingRule shardingRule, DataSourceRule dataSourceRule){
        this.threadLocalObjectContainer = new ThreadLocalObjectContainer();
        Preconditions.checkNotNull(shardingRule);
//        Preconditions.checkNotNull(props);
        ShardingConfiguration configuration = new ShardingConfiguration(new Properties());
        this.initThreadLocalObjectContainer(configuration);

        DatabaseType type;
        try {
            type = DatabaseType.valueFrom(ShardingConnection.getDatabaseMetaDataFromDataSource(shardingRule.getDataSourceRule().getDataSources()).getDatabaseProductName());
        } catch (SQLException var6) {
            throw new ShardingJdbcException("Can not get database product name", var6);
        }

        this.context = new ShardingContext(shardingRule, new SQLRouteEngine(shardingRule, type), new ExecutorEngine(configuration));

        this.dataSourceRule = dataSourceRule;
    }

    private void initThreadLocalObjectContainer(ShardingConfiguration configuration) {
        if(((Boolean)configuration.getConfig(ShardingConfigurationConstant.METRICS_ENABLE, Boolean.TYPE)).booleanValue()) {
            this.threadLocalObjectContainer.initItem(new MetricsContext(((Long)configuration.getConfig(ShardingConfigurationConstant.METRICS_SECOND_PERIOD, Long.TYPE)).longValue(), (String)configuration.getConfig(ShardingConfigurationConstant.METRICS_PACKAGE_NAME, String.class)));
        }

    }

    public SparkJDBCConfigure route(String sql2Route, Object... parameters){
        ArrayList<SparkJDBCConfigure> result = new ArrayList();
        SQLRouteResult sqlRouteResult = this.context.getSqlRouteEngine().route(sql2Route, Arrays.asList(parameters));
        Iterator i$ = sqlRouteResult.getExecutionUnits().iterator();
        while(i$.hasNext()) {
            SQLExecutionUnit each = (SQLExecutionUnit)i$.next();
//            PreparedStatement preparedStatement = this.generatePrepareStatement(this.getShardingConnection().getConnection(each.getDataSource()), each.getSql());
//            this.replayMethodsInvovation(preparedStatement);
//            this.setParameters(preparedStatement, parameters);
//            result.add(preparedStatement);
//            System.out.print(each.getDataSource()+ "\t");
//            System.out.println(each.getSql());
            DruidAbstractDataSource dataSource = (DruidAbstractDataSource)this.dataSourceRule.getDataSource(each.getDataSource());

            result.add(new SparkJDBCConfigure(dataSource.getUrl(), setParamters(each.getSql(), parameters), dataSource.getUsername(), dataSource.getPassword()));
        }

        if(result.size() != 1)
            throw new RuntimeException("无法定位数据表");
        return result.get(0);
    }

    public String setParamters(String sql, Object... parameters){
        String[] sqlSplits = sql.split("\\?");
        if ((sql.endsWith("?") && sqlSplits.length != parameters.length ) || (!sql.endsWith("?") && sqlSplits.length != parameters.length + 1)){
            throw new IllegalArgumentException();
        }
        StringBuilder sb = new StringBuilder();
        for(int i = 0; i<parameters.length; i++){
            sb.append(sqlSplits[i]).append("'").append(String.valueOf(parameters[i])).append("'");
        }
        return sb.toString();
    }



}
