package com.bigdata.utils.db.rdbms;


/**
 * @author MAIBENBEN
 * @Date 2018/11/25 16:22
 */
public enum SaveMode {

    /**
     * 采用REPLACE INTO 方式插入
     */
    Replace,
    /**
     * 在插入数据前会使用 TRUNCATE TABLE 将表里的数据清空
     * 然后使用 INSERT INTO 方式插入
     */
    Overwrite
}
