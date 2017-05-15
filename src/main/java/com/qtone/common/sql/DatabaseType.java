package com.qtone.common.sql;

/**
 * Created by kwong on 2017/5/10.
 */
public enum DatabaseType {

    Mysql(2, "mysql"),
    Oracle(3, "oracle");

    private int code;

    private String name;

    DatabaseType(int code, String name){
        this.code = code;
        this.name = name;
    }

    public int getCode() {
        return code;
    }

    public String getName() {
        return name;
    }
}
