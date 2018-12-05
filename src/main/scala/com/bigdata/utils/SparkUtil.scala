package com.bigdata.utils

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Mr.Zhu
  *
  **/
object SparkUtil {

  /**
    * 根据运行环境获取SparkContext
    * 不设置为生产环境模式
    * 设置为本地测试模式
    *
    * @param appName 应用程序名称
    * @return
    */
  def getSparkContext(appName: String): SparkContext = {
    val conf = new SparkConf()
      .setAppName(appName)
    if (System.getProperty("os.name").toLowerCase.startsWith("win")) {
      conf.setMaster("local[6]")
    }
    new SparkContext(conf)
  }

  /**
    * 获取根据tableName 获取数据集
    *
    * @param sqlContext SQLContext
    * @param tableName  表名
    * @param prop       数据库连接配置
    * @return
    */
  def getDF(sqlContext: SQLContext,
            tableName: String,
            prop: Properties): DataFrame = {
    sqlContext.read.jdbc(prop.getProperty("url"), tableName, prop)
  }

}
