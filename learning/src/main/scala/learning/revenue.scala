package learning

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._

object revenue {
  
  def main(args: Array[String]){ 
    
    val spark:SparkSession = SparkSession.builder().appName("revenueAnalysis")
                                         .master("local[*]")
                                         .getOrCreate()
                                         
    /*
     * 	Total Revenue Analysis
				Highest Value Order
				Top-Spending Customer
				Order Count per Customer                                     
     */
              
    println(spark)
    spark.sparkContext.setLogLevel("ERROR")
    
    val data = Seq((1,"Customer1","2023-01-10",100),
                   (2,"Customer2","2023-01-11",150),
                   (3,"Customer1","2023-01-12",80),
                   (4,"Customer3","2023-01-12",190),
                   (5,"Customer2","2023-01-13",50))
                   
   import spark.implicits._
   
   val df = spark.sparkContext.parallelize(data)
                 .toDF("order_id","customer_id","order_date","total_amount")
                 
   df.show()
   df.printSchema()
   
   /* Total Revenue Analysis */
   df.agg(sum(col("total_amount")).alias("total revenue")).show()
   
   /* Highest Value Order */
   df.agg(max(col("total_amount")).alias("maximum order value")).show()
   
   /* Top Spending Customer */
   df.groupBy("customer_id").agg(sum(col("total_amount")).alias("total_spending"))
     .orderBy(col("total_spending").desc).show(1)
   
   /* Order count per customer */
   df.groupBy("customer_id").count().show()
   
   
  }
  
}