package org.spark.practice

import org.apache.spark.sql.SparkSession

object pivot {
  
  def main(args: Array[String]){ 
    
    val spark = SparkSession.builder().appName("pivotjob").master("local[*]").getOrCreate()
    
    println(spark)
    
  }
  
}