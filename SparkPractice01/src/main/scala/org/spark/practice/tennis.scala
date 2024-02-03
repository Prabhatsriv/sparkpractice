package org.spark.practice
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object tennis {
  def main(args:Array[String]){ 
    
 /* Tennis Match Question 
 * Find out the Total Match wins by each player in all event 
 * Find out the total matches wins by each player in each year
 */
    
    val spark = SparkSession.builder().master("local[1]").appName("tennisCode").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    val player_data = Seq((1,"Nadal"),(2,"Federer"),(3,"Novak"))
    val match_data = Seq((2017,2,1,1,3),(2018,3,2,3,2),(2019,3,1,2,3))
    
    import spark.implicits._ 
    
    val player_df = spark.createDataFrame(player_data).toDF("Player_ID","Player_Name")
    val match_df  = spark.createDataFrame(match_data).toDF("Year","Wimberldon","Fr_Open","US_Open","Aus_Open")
    
    println("-----Player Details ------") 
    player_df.show()
    player_df.printSchema()
    
    println("-----Match Details ------") 
    match_df.show()
    match_df.printSchema()
    
    val union_df = match_df.select("Year","Wimberldon").unionAll(match_df.select("Year","Fr_Open"))
                                 .unionAll(match_df.select("Year","US_Open"))
                                 .unionAll(match_df.select("Year","Aus_Open"))
            .withColumnRenamed("Wimberldon","Player_ID")
    
            
    val total_win_df = union_df.groupBy("Player_ID").count().orderBy("Player_ID")    
    val total_win_per_year_df = union_df.groupBy("Player_ID","Year").count().orderBy("Player_ID","Year")
    
    println("-----Total Match won by each Player -----") 
    total_win_df.join(player_df,Seq("Player_ID"),"inner")
                .select(col("Player_Name"),col("count").as("Win")).show()
    
    println("-----Total Match won by each Player in each year -----")
   
    total_win_per_year_df.join(player_df,Seq("Player_ID"),"inner")
                         .select(col("Year"),col("Player_Name"),col("count").as("Win")).show()
  }
}



