package com.spark.transform

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

object readFromPostgres {
  def transform(spark: SparkSession, jdbcUrl: String, tableName: String, props: Properties): DataFrame = {
    spark.read.jdbc(jdbcUrl, tableName, props)
  }
}
