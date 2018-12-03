package com.bigdata.spark_hbase

import com.bigdata.utils.SparkUtil
import com.bigdata.utils.db.nosql.HbaseUtil

/**
  * @author   Mr.Zhu
  *
  **/
object Spark_Read_Hbase {
  def main(args: Array[String]): Unit = {
    val sc = SparkUtil.getSparkContext(Spark_Read_Hbase.getClass.getSimpleName)
    val inPutTable = ""
    val snapshotName = ""
    val snapshotPath = ""
    val fields = List("")
    val hbaseRDD = HbaseUtil.getHbaseRDD(sc, inPutTable, snapshotName, snapshotPath, fields)
    println(hbaseRDD.count())
    sc.stop()
  }
}
