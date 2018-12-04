package com.bigdata.utils.db.rdbms

import java.sql.{Connection, PreparedStatement, SQLException}

import org.apache.spark.sql.types.StructType

import scala.util.Try

/**
  * @author Mr.Zhu
  * @version 10=.0
  **/
object TableWriteUtil {
  /**
    * 创建表
    *
    * @param rddSchema  DF数据结构
    * @param conn       数据库连接
    * @param table      表名
    * @return
    */
  def createTable(rddSchema: StructType, conn: Connection, table: String): Boolean = {
    Try {
      val sb = new StringBuilder()
      rddSchema.fields foreach { field => {
        val name = field.name
        val typ: String = field.dataType.toString
        val nullable = if (field.nullable) "" else "NOT NULL"
        sb.append(s", $name $typ $nullable")
      }
      }
      if (sb.length < 2) {
        throw new SQLException(s"Can't find the field information of $table table")
      }
      val sql = s"CREATE TABLE $table ($sb.substring(2))"
      val statement = conn.prepareStatement(sql)

      try {
        statement.executeQuery()
      } finally {
        statement.close()
      }
    }.isSuccess
  }

  /**
    * 查看表是否存在
    *
    * @param conn     数据库连接
    * @param table    表名
    * @return
    */
  def tableExists(conn: Connection, table: String): Boolean = {
    Try {
      val statement = conn.prepareStatement(s"SELECT * FROM $table WHERE 0=1")
      try {
        statement.executeQuery()
      } finally {
        statement.close()
      }
    }.isSuccess
  }

  /**
    * 删除表
    *
    * @param conn   数据库连接
    * @param table  表名
    * @return
    */
  def dropTable(conn: Connection, table: String): Boolean = {
    Try {
      val statement = conn.prepareStatement(s"DROP TABLE $table")
      try {
        statement.executeUpdate()
      } finally {
        statement.close()
      }
    }.isSuccess
  }

  /**
    * 清空表
    *
    * @param conn   数据库连接
    * @param table  表名
    * @return
    */
  def truncateTable(conn: Connection, table: String): Boolean = {
    Try {
      val statement = conn.prepareStatement(s"TRUNCATE TABLE $table")
      try {
        statement.executeUpdate()
      } finally {
        statement.close()
      }
    }.isSuccess
  }

  /**
    * INSERT INTO 语句
    * @param conn        数据库连接
    * @param table       表名
    * @param rddSchema   字段信息
    * @return
    */
  def insertStatement(conn: Connection, table: String, rddSchema: StructType): PreparedStatement = {
    val columns = rddSchema.fields.map(_.name).mkString(",")
    val placeholders = rddSchema.fields.map(_ => "?").mkString(",")
    val sql = s"INSERT INTO $table ($columns) VALUES ($placeholders)"
    conn.prepareStatement(sql)
  }

  /**
    * REPLACE INTO 语句
    * @param  conn        数据库连接
    * @param  table       表名
    * @param  rddSchema   字段信息
    * @return
    */
  def replaceStatement(conn: Connection, table: String, rddSchema: StructType): PreparedStatement = {
    val columns = rddSchema.fields.map(_.name).mkString(",")
    val placeholders = rddSchema.fields.map(_ => "?").mkString(",")
    val sql = s"REPLACE INTO $table ($columns) VALUES ($placeholders)"
    conn.prepareStatement(sql)
  }

}
