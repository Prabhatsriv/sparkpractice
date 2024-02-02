package org.spark.practice
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType,StructField,StringType,IntegerType}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{sum,col}
import org.apache.spark.rdd


object hire {
  
  def main(args:Array[String]) { 
    
  /* Hire Candidate
   * Hire the candidates who fall under budget of 70000 according to below criteria:
   * First hire Senior within budget then hire Junior within remaining budget.
   */
   
    val spark:SparkSession = SparkSession.builder().appName("hire").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    val data = Seq(Row(1,"junior",10000),Row(1,"junior",15000),Row(1,"junior",40000),
                   Row(4,"senior",16000),Row(5,"senior",20000),Row(6,"senior",50000))
                 
    val schema = StructType(Array(StructField("empid",IntegerType,nullable = true),
                                  StructField("position",StringType,nullable = true),
                                  StructField("salary",IntegerType,nullable = true)))
                                  
    val candidate_df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    
    
    println("=========== candidate dataframe =============== ")
    candidate_df.show()
    
    val windowspec = Window.partitionBy("position").orderBy("salary","empid")
    
    val running_salary_df = candidate_df.withColumn("running_salary", sum("salary").over(windowspec))
    
    println("=========== candidate with running salary =============== ")
    running_salary_df.show()
    
    val senior_df = running_salary_df.where(col("position") === "senior" 
                                             && col("running_salary") < 70000)
                                     .select("empid","position","salary")
    println()
    println("=========== senior candidate  =============== ")
    senior_df.show()
    
    //converting senior budget dataframe into rdd first 
    //then using map and mkString function, converting into Integer by collect
    val senior_budget = senior_df.groupBy().sum("salary").as("max_salary").rdd
                                 .map( x => x.mkString(" ").toInt).collect()(0)
                                     
    val remaining_budget = 70000 - senior_budget 
    
    
    val junior_df = running_salary_df.where(col("position") === "junior" 
                                            && col("running_salary") < remaining_budget)
                                     .select("empid","position","salary")
    println()
    println("=========== junior candidate  =============== ")                            
    junior_df.show()
    
    
    println("=============== final output =================== ")
    senior_df.union(junior_df).show()
    

    
    
    
    
  }
  
}