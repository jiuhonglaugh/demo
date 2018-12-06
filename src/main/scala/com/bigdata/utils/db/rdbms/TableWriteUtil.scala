package com.bigdata.utils.db.rdbms

import java.sql.{Connection, PreparedStatement, SQLException}

import org.apache.spark.sql.types._
import org.slf4j.{Logger, LoggerFactory}

import scala.util.Try

/**
  * @author Mr.Zhu
  * @version 10=.0
  **/
object TableWriteUtil {

  private val LOGGER: Logger = LoggerFactory.getLogger(TableWriteUtil.getClass)

  /**
    * 创建表
    *
    * @param rddSchema  DF数据结构
    * @param conn       数据库连接
    * @param table      表名
    * @return
    */
  def createTable(rddSchema: StructType, conn: Connection, table: String): Unit = {
    val sb = new StringBuilder()
    rddSchema.fields foreach { field => {
      val name = field.name
      val typ: String = getType(field.dataType)
      val nullable = if (field.nullable) "" else "NOT NULL"
      sb.append(s", $name $typ $nullable")
    }
    }
    if (sb.length < 2) {
    }
    val sql = s"CREATE TABLE $table (${sb.substring(2)})"

    val statement = conn.prepareStatement(sql)
    LOGGER.info(s"$table Table Create SUCCESS")
    try {
      statement.execute()
    } catch {
      case e: SQLException => LOGGER.error(e.toString); throw new SQLException(s"$table Table Create Failed")
      case e: Exception => LOGGER.error(e.toString); throw new SQLException(s"$table Table Create Failed")
    } finally {
      statement.close()
    }
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
    * @param conn  数据库链接
    * @param table 表名
    * @return
    */
  def dropTable(conn: Connection, table: String): Unit = {
    val statement = conn.prepareStatement(s"DROP TABLE $table")
    try {
      statement.executeUpdate()
      LOGGER.warn(s"$table Table Drop SUCCESS")
    } catch {
      case e: SQLException => LOGGER.error(e.toString); throw new SQLException(s"$table Table Drop Failed")
      case e: Exception => LOGGER.error(e.toString); throw new SQLException(s"$table Table Drop Failed")
    } finally {
      statement.close()
    }
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
 
 /**
    * 根据数据类型进行模式匹配
    *
    * @param fileType 字段数据类型
    * @return
    */
  private def getType(fileType: DataType): String = {
    fileType match {
      case IntegerType => "int(10)"
      case LongType => "BigInt(20)"
      case DoubleType => "double(10,2)"
      case FloatType => "float(10,2)"
      case ShortType => "year"
      case ByteType => "blob"
      case BooleanType => "boolean"
      case StringType => "text"
      case BinaryType => "binary(3)"
      case TimestampType => "datetime"
      case DateType => "date"
      case _: DecimalType => "decimal(10,2)"
      case _ => throw new IllegalArgumentException(
        s"Can't translate non-null value for field $fileType")
    }
  }

}
