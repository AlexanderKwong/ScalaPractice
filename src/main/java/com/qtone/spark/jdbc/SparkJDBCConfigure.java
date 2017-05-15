package com.qtone.spark.jdbc;

/**
 * Created by kwong on 2017/5/9.
 */
public class SparkJDBCConfigure {

    private String url;

    private String sql;

    private String user;

    private String password;

    private String driver;

    private Integer lowerBound;

    private Integer upperBound;

    private Integer fetchSize;

    public SparkJDBCConfigure(String url, String sql, String user, String password, String driver, Integer lowerBound, Integer upperBound, Integer fetchSize) {
        this.url = url;
        this.sql = sql;
        this.user = user;
        this.password = password;
        this.driver = driver;
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
        this.fetchSize = fetchSize;
    }

    public SparkJDBCConfigure(String url, String sql, String user, String password) {
        this(url, sql, user, password, "com.mysql.jdbc.Driver", 0, 0, 1000);
    }

    public String getUrl() {
        return url;
    }

    public String getSql() {
        return sql;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    public String getDriver() {
        return driver;
    }

    public Integer getLowerBound() {
        return lowerBound;
    }

    public Integer getUpperBound() {
        return upperBound;
    }

    public Integer getFetchSize() {
        return fetchSize;
    }
}
