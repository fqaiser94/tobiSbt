package com.spark.transform

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait SparkSessionTestWrapper {
  val conf: SparkConf = new SparkConf()
    .setMaster("local[*]")
    .set("spark.driver.memory", "1g")

  lazy val spark: SparkSession = SparkSession.builder
    .master("local[*]")
    .config(conf)
    .getOrCreate()
}