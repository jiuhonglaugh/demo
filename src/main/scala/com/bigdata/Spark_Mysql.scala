package com.bigdata

import com.bigdata.utils.SparkUtil
import com.bigdata.utils.db.rdbms.{DbcpUtil, RdbmsUtil, SaveMode}
import org.apache.spark.sql.SQLContext

/**
  * @author Mr.Zhu
  * @version 1.0
  **/
object Spark_Mysql {
  def main(args: Array[String]): Unit = {

    val inputTable = ""
    val sc = SparkUtil.getSparkContext(Spark_Mysql.getClass.getSimpleName)
    val outputTable = ""

    val sqlContext = SQLContext.getOrCreate(sc)
    val data = SparkUtil.getDF(sqlContext, inputTable, DbcpUtil.getProp)

    val rdbmsUtil = new RdbmsUtil(SaveMode.Replace)
    rdbmsUtil.saveTable(data,outputTable)

  }
}
