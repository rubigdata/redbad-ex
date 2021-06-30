package org.rubigdata

import org.apache.spark.sql.SparkSession

object CCIndexEx {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("CCIndexEx").getOrCreate()
    val data = spark.read
	.option("mergeSchema", "true")
	.option("enable.summary-metadata", "false")
	.option("enable.dictionary", "true")
	.option("filterPushdown", "true")
	.parquet("hdfs://gelre/user/core/cc-index-nl")
    data.show(10,false)
    spark.stop()
  }
}
