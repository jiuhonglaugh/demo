package com.bigdata.utils.db.rdbms

import java.io.IOException
import java.sql.{Connection, SQLException}
import java.util.Properties
import javax.sql.DataSource
import org.apache.commons.dbcp.BasicDataSourceFactory
import org.slf4j.{Logger, LoggerFactory}

/**
  * @author Mr.Zhu
  * @version 1.0
  **/
object DbcpUtil {
  private val LOGGER: Logger = LoggerFactory.getLogger(DbcpUtil.getClass)
  private val properties = new Properties()
  private var dataSource: DataSource = _
  try {
    val is = Thread.currentThread().getContextClassLoader.getResourceAsStream("dbcp.properties")
    properties.load(is)
  } catch {
    case e: IOException => LOGGER.error(e.toString)
  }
  try {
    dataSource = BasicDataSourceFactory.createDataSource(properties)
  } catch {
    case e: Exception => LOGGER.error(e.toString)
  }

  def getConnection: Connection = {
    var connection: Connection = null
    try {
      connection = dataSource.getConnection()
    } catch {
      case e: SQLException => LOGGER.error(e.toString)
    }
    connection
  }
}
