package com.bigdata.spark_hbase

import java.util
import java.util.Properties
import com.bigdata.sort.BulkLoadSort
import com.bigdata.utils.SparkUtil
import com.bigdata.utils.db.nosql.HbaseUtil
import com.bigdata.utils.db.rdbms.TableReadUtil
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.spark.sql.SQLContext

/**
  * @author   Mr.Zhu
  *
  **/
object Spark_Write_Hbase {
  def main(args: Array[String]): Unit = {
    val sc = SparkUtil.getSparkContext(Spark_Write_Hbase.getClass.getSimpleName)
    val sqlContext = new SQLContext(sc)
    val inPutTable = "test"
    val url = ""
    val prpo = new Properties()
    val outPutTable = "test"
    val outPutPath = "hdfs 路径"
    val df = TableReadUtil.getDF(sqlContext, url, inPutTable, prpo)
    val offsert = 0 //这个值是你的rowkey在你查询后字段的位置
    val hbaseRDD = HbaseUtil.hfileKV(df, df.schema, offsert, new util.HashMap[String, String]())
    val hbaseUtil = new HbaseUtil(outPutTable, outPutPath)
    hbaseRDD.sortBy(x => BulkLoadSort(x._1, x._2))
      .saveAsNewAPIHadoopFile(outPutPath,
        classOf[ImmutableBytesWritable],
        classOf[KeyValue],
        classOf[HFileOutputFormat2],
        hbaseUtil.getConf)
    hbaseUtil.bulkLoader()
    sc.stop()
  }
}
