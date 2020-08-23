package org.example

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession


/**
 * Test IO to wasb
 */
object App {
  def main (arg: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("ADF demo")
      .getOrCreate()

    val connectionProperties = new Properties()
    connectionProperties.put("user", "*****")
    connectionProperties.put("password", "******")
    connectionProperties.put("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")

    val jdbc_url = "jdbc:sqlserver://adf-staging-db-server.database.windows.net:1433;database=staging-db";
    val sqlTableDF = spark.read.jdbc(jdbc_url, "recipesInfo", connectionProperties)

    sqlTableDF.printSchema
  }
}

