package com.bigdata.utils.db.rdbms

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * @author Mr.Zhu
  *
  **/
object TableReadUtil {
  /**
    * 获取根据tableName 获取数据集
    *
    * @param sqlContext SQLContext
    * @param url        数据库地址
    * @param tableName  表名
    * @param prop       数据库连接配置
    * @return
    */
  def getDF(sqlContext: SQLContext,
            url: String,
            tableName: String,
            prop: Properties): DataFrame = {
    sqlContext.read.jdbc(url, tableName, prop)
  }
}
