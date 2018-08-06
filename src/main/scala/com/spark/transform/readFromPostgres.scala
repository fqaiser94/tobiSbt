package com.spark.transform

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

object readFromPostgres {
  def transform(spark: SparkSession, jdbcUrl: String, tableName: String, props: Properties): DataFrame = {
    spark.read.jdbc(jdbcUrl, tableName, props)
  }

  def genProps(user: String, password: String, driver: String): Properties = {
    val props = new Properties()
    props.put("user", user)
    props.put("password", password)
    props.put("driver", driver)

    props
  }
}
