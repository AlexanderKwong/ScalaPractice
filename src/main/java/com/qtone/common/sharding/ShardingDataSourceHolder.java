package com.qtone.common.sharding;

import com.dangdang.ddframe.rdb.sharding.api.ShardingDataSource;
import com.dangdang.ddframe.rdb.sharding.jdbc.adapter.AbstractDataSourceAdapter;
import com.qtone.common.spring.SpringUtil;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author 邝晓林
 * @Description
 * @date 2017/3/13
 */
public class ShardingDataSourceHolder extends AbstractDataSourceAdapter{

    private static ShardingDataSource shardingDataSource = SpringUtil.getSpringBean(null, "shardingDataSource");

    @Override
    public Connection getConnection() throws SQLException {
       return shardingDataSource.getConnection();
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return shardingDataSource.getConnection(username, password);
    }
}
