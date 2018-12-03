package com.bigdata.sort

import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.io.ImmutableBytesWritable

/**
  * @author Mr.Zhu
  * 写 Hfile 是时需要保证数据是有序的
  * 顺序依次为 key 、rowkey、family、filed
  * 所以下面的排序按照此顺序依次对比
  **/
case class BulkLoadSort(key: ImmutableBytesWritable, value: KeyValue) extends Ordered[BulkLoadSort] {
  override def compare(that: BulkLoadSort): Int = {
    if (this.key != that.key) {
      this.key.compareTo(that.key)
    } else {
      val thisA = this.value.toStringMap
      val thatB = that.value.toStringMap
      val thisClo = thisA.get("family").toString
      val thatClo = thatB.get("family").toString
      if (!thisClo.equals(thatClo)) {
        thisClo.compareTo(thatClo)
      } else {
        thisA.get("qualifier").toString.compareTo(thatB.get("qualifier").toString)
      }
    }
  }
}
