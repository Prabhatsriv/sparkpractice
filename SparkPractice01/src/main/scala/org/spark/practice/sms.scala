package org.spark.practice
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object sms {

  def main(args: Array[String]) {

    val spark: SparkSession = SparkSession.builder().appName("smsCode").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    /*
     * Get the total number of sms exchanged between two people.
     */
    val sms_schema = new StructType().add("sms_date",StringType,true)
                                     .add("sender",StringType,true)
                                     .add("receiver",StringType,true)
                                     .add("sms_no",IntegerType,true)
                                    
    val infile = args(0)
    val input_data = spark.read.format("csv")
                               .option("header", "true")
                               .schema(sms_schema)
                               .load(infile)

    println("======== input sms data and schema ==========")
    input_data.printSchema()
    input_data.show()

    val outDf = input_data.withColumn("person1",
                                      when(col("sender") < col("receiver"), col("receiver"))
                                      .otherwise(col("sender")))
                          .withColumn("person2", 
                                      when(col("sender") < col("receiver"), col("sender"))
                                     .otherwise(col("receiver")))
                               
   
    println("======== output data ================")
    outDf.groupBy("person1","person2").agg(sum("sms_no").as("total number of sms"))
         .orderBy("total number of sms")
         .show()

  }
}