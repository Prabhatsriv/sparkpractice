package org.spark.practice

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{sum}

object pivot {
  
  def main(args: Array[String]){ 
    
    val spark = SparkSession.builder().appName("pivotCode").master("local[*]").getOrCreate()
    
    val data = Seq(("Banana",1000,"USA"), ("Carrots",1500,"USA"), ("Beans",1600,"USA"),
      ("Orange",2000,"USA"),("Orange",2000,"USA"),("Banana",400,"China"),
      ("Carrots",1200,"China"),("Beans",1500,"China"),("Orange",4000,"China"),
      ("Banana",2000,"Canada"),("Carrots",2000,"Canada"),("Beans",2000,"Mexico"))

    import spark.sqlContext.implicits._
    
    val df = data.toDF("Product","Amount","Country")
    df.show()
    
    /* Pivot the data */ 
    val countries = Seq("USA","China","Canada","Mexico")
    
    df.groupBy("Product").pivot("Country", countries).agg(sum("Amount").alias("Sum")).show()
    
  }
  
}